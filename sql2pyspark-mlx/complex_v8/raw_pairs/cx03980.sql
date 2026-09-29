WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 79.7
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY quality, site_id ORDER BY voltage, id))) / 60, 1) AS minutes_since_prev_read_at,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage
    FROM valid_readings
    WINDOW w_site_id AS (PARTITION BY site_id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY distinct_quality_in_site_id ORDER BY minutes_since_prev_read_at, id) AS minutes_since_prev_read_at_rank,
        LEAD(site_id_sum_voltage) OVER w_distinct_quality_in_site_id AS next_site_id_sum_voltage,
        distinct_quality_in_site_id - LAG(distinct_quality_in_site_id) OVER (PARTITION BY distinct_quality_in_site_id ORDER BY minutes_since_prev_read_at, id) AS distinct_quality_in_site_id_change
    FROM windowed
    WINDOW w_distinct_quality_in_site_id AS (PARTITION BY distinct_quality_in_site_id ORDER BY minutes_since_prev_read_at, id)
)
SELECT *
FROM (
    SELECT read_at, minutes_since_prev_read_at_rank, quality, voltage
    FROM metrics
) src
PIVOT (
    COUNT(voltage) AS count_v, MAX(voltage) AS max_v
    FOR quality IN ('BAD', 'EST', 'OK')
)
