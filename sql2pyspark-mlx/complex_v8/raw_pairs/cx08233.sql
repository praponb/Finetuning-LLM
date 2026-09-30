WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.read_at_year,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM prepared s
    JOIN meter_readings m ON m.site_id = s.site_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(read_at) OVER (ORDER BY read_at_year, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        FIRST_VALUE(reading_read_at) IGNORE NULLS OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY reading_read_at ASC NULLS LAST, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reading_read_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at_year, id, reading_id), 3) AS read_at_year_percent_rank,
        ROUND(AVG(read_at_year) OVER (ORDER BY read_at_year, id, reading_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS read_at_year_avg_3_prior,
        ROUND(read_at_year / NULLIF(SUM(read_at_year) OVER (PARTITION BY reading_id, read_at_year), 0), 4) AS read_at_year_share_of_reading_id_read_at_year,
        ROUND(CUME_DIST() OVER (ORDER BY voltage DESC, id, reading_id), 3) AS voltage_cume_dist
    FROM detail_rows_readings
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        voltage,
        reading_id,
        reading_read_at,
        reading_quality,
        last_read_at,
        first_known_reading_read_at,
        read_at_year_avg_3_prior,
        read_at_year_share_of_reading_id_read_at_year,
        voltage_cume_dist,
        SIZE(COLLECT_SET(reading_quality) OVER w_first_known_reading_read_at) AS distinct_reading_quality_in_first_known_reading_,
        LAST_VALUE(read_at_year_avg_3_prior) OVER (PARTITION BY read_at_year ORDER BY read_at, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_year_avg_3_prior,
        LAG(voltage) OVER (PARTITION BY read_at_year ORDER BY read_at, id, reading_id) AS prev_voltage,
        COUNT(*) OVER (PARTITION BY MONTH(first_known_reading_read_at)) AS rows_in_first_known_reading_read_at_month,
        RANK() OVER (ORDER BY voltage ASC) AS voltage_rank,
        ROUND(voltage_cume_dist / NULLIF(SUM(voltage_cume_dist) OVER (PARTITION BY MONTH(first_known_reading_read_at)), 0), 4) AS voltage_cume_dist_share_of_first_known_reading_r
    FROM windowed
    WINDOW w_first_known_reading_read_at AS (PARTITION BY MONTH(first_known_reading_read_at))
)
SELECT *
FROM (
    SELECT voltage_rank, reading_id, reading_quality, read_at_year_avg_3_prior
    FROM windowed_2
) src
PIVOT (
    MIN(read_at_year_avg_3_prior)
    FOR reading_quality IN ('OK', 'EST', 'BAD')
)
ORDER BY voltage_rank, reading_id
