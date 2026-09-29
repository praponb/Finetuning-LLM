WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE (MONTH(read_at) IN (1, 2) OR site_id IS NOT NULL)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, SUM(kwh) AS total_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.total_reading_kwh
    FROM recent_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        kwh - LAG(kwh) OVER (ORDER BY read_at_dayofweek, id) AS kwh_change,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        ROUND(total_reading_kwh / NULLIF(SUM(total_reading_kwh) OVER w_site_id, 0), 4) AS total_reading_kwh_share_of_site_id,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id
    FROM summarized_rows
    WINDOW w_site_id AS (PARTITION BY site_id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        site_id_min_kwh,
        kwh_change,
        kwh_vs_site_id_avg,
        total_reading_kwh_share_of_site_id,
        distinct_quality_in_site_id,
        NTH_VALUE(kwh_change, 2) OVER (PARTITION BY site_id ORDER BY total_reading_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_change,
        COUNT(*) OVER (PARTITION BY quality, read_at_dayofweek) AS rows_in_quality_read_at_dayofweek,
        FIRST_VALUE(read_at_dayofweek) OVER (PARTITION BY site_id ORDER BY total_reading_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at_dayofweek
    FROM windowed
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage > 216.8
        )
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, ROUND(AVG(kwh), 2) AS avg_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.avg_reading_kwh
    FROM qualified s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    site_id_min_kwh,
    kwh_change,
    kwh_vs_site_id_avg,
    total_reading_kwh_share_of_site_id,
    rows_in_quality_read_at_dayofweek,
    first_read_at_dayofweek,
    reading_count,
    avg_reading_kwh,
    DAYOFWEEK(read_at) AS read_at_dayofweek
FROM with_stats
WHERE site_id >= 5
ORDER BY first_read_at_dayofweek DESC, kwh NULLS LAST, id
LIMIT 5
