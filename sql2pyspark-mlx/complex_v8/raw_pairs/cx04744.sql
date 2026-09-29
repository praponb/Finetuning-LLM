WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.kwh) AS prior_kwh_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        prior_kwh_7d,
        TO_DATE(read_at) AS read_at_date
    FROM matched
    WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT COUNT(CASE WHEN m.quality = 'BAD' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM annotated s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        prior_kwh_7d,
        read_at_date,
        max_reading_voltage,
        reading_quality_count,
        reading_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY prior_kwh_7d DESC), 3) AS prior_kwh_7d_percent_rank,
        FIRST_VALUE(read_at_date) IGNORE NULLS OVER (ORDER BY prior_kwh_7d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_date,
        COUNT(*) OVER (PARTITION BY reading_count, site_id) AS rows_in_reading_count_site_id,
        ROUND(CUME_DIST() OVER w_all_by_prior_kwh_7d_ties, 3) AS prior_kwh_7d_cume_dist,
        LAST_VALUE(read_at) OVER (ORDER BY prior_kwh_7d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at
    FROM profiled
    WINDOW w_all_by_prior_kwh_7d_ties AS (ORDER BY prior_kwh_7d DESC)
)
SELECT
    id,
    site_id,
    prior_kwh_7d,
    max_reading_voltage,
    reading_count,
    prior_kwh_7d_percent_rank,
    first_known_read_at_date,
    rows_in_reading_count_site_id,
    prior_kwh_7d_cume_dist
FROM metrics
