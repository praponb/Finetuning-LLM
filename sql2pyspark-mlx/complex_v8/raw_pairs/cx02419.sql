WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND kwh <= 109.0
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM prepared_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(reading_kwh) OVER (PARTITION BY reading_id, site_id) AS reading_id_site_id_max_reading_kwh,
        SIZE(COLLECT_SET(reading_quality) OVER (PARTITION BY reading_id, site_id)) AS distinct_reading_quality_in_reading_id_site_id,
        ROW_NUMBER() OVER w_reading_id_reading_quality_b AS reading_kwh_seq,
        MIN(reading_voltage) OVER (PARTITION BY reading_id, site_id) AS reading_id_site_id_min_reading_voltage,
        NTH_VALUE(reading_voltage, 2) OVER (PARTITION BY reading_id, reading_quality ORDER BY reading_kwh, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_voltage,
        ROUND(AVG(reading_voltage) OVER (PARTITION BY reading_id, reading_quality ORDER BY reading_kwh, id, reading_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS reading_voltage_avg_3_prior
    FROM line_level
    WINDOW w_reading_id_reading_quality_b AS (PARTITION BY reading_id, reading_quality ORDER BY reading_kwh, id, reading_id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        reading_kwh_seq,
        distinct_reading_quality_in_reading_id_site_id,
        COUNT(*) FILTER (WHERE reading_quality IS NOT NULL) AS n_reading_quality_is_not_null,
        MAX(reading_voltage_avg_3_prior) AS max_reading_voltage_avg_3_prior,
        MAX(reading_kwh) AS max_reading_kwh,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(reading_kwh_seq) AS min_reading_kwh_seq
    FROM with_windows
    GROUP BY reading_kwh_seq, distinct_reading_quality_in_reading_id_site_id
    HAVING AVG(distinct_reading_quality_in_reading_id_site_id) > 2
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE n_reading_quality_is_not_null < (SELECT MAX(n_reading_quality_is_not_null) FROM stats)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(max_reading_kwh / NULLIF(SUM(max_reading_kwh) OVER (PARTITION BY distinct_reading_quality_in_reading_id_site_id), 0), 4) AS max_reading_kwh_share_of_distinct_reading_qualit,
        ROUND(CUME_DIST() OVER (PARTITION BY n_reading_quality_is_not_null, reading_kwh_seq ORDER BY min_reading_kwh_seq DESC), 3) AS min_reading_kwh_seq_cume_dist,
        COUNT(*) OVER (PARTITION BY distinct_reading_quality_in_reading_id_site_id) AS rows_in_distinct_reading_quality_in_reading_id_s,
        LAG(max_reading_kwh) OVER w_n_reading_quality_is_not_nul_2 AS prev_max_reading_kwh,
        NTILE(10) OVER (ORDER BY n_reading_quality_is_not_null ASC, reading_kwh_seq, distinct_reading_quality_in_reading_id_site_id) AS n_reading_quality_is_not_null_decile,
        DENSE_RANK() OVER w_min_reading_kwh_seq_reading AS max_reading_voltage_avg_3_prior_dense_rank
    FROM qualified
    WINDOW w_n_reading_quality_is_not_nul_2 AS (PARTITION BY n_reading_quality_is_not_null, reading_kwh_seq ORDER BY min_reading_kwh_seq DESC, reading_kwh_seq, distinct_reading_quality_in_reading_id_site_id), w_min_reading_kwh_seq_reading AS (PARTITION BY min_reading_kwh_seq, reading_kwh_seq ORDER BY max_reading_voltage_avg_3_prior)
)
-- Final projection
SELECT
    reading_kwh_seq,
    distinct_reading_quality_in_reading_id_site_id,
    n_reading_quality_is_not_null,
    max_reading_kwh,
    distinct_ids,
    min_reading_kwh_seq,
    max_reading_kwh_share_of_distinct_reading_qualit,
    prev_max_reading_kwh,
    n_reading_quality_is_not_null_decile,
    max_reading_voltage_avg_3_prior_dense_rank
FROM windowed
ORDER BY reading_kwh_seq DESC NULLS FIRST, prev_max_reading_kwh DESC NULLS FIRST
