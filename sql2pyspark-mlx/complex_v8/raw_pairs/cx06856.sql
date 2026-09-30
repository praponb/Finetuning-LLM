WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR read_at < TIMESTAMP '2025-02-18 12:00:00')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS voltage_vs_read_at_year_site_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY voltage NULLS LAST), 3) AS voltage_cume_dist,
        RANK() OVER w_all_by_voltage_ties AS voltage_rank,
        ROUND(CUME_DIST() OVER w_site_id_quality_by_read_at_t, 3) AS read_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY voltage ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        LAST_VALUE(read_at) OVER (PARTITION BY site_id, quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        LEAD(read_at, 2) OVER w_quality_by_voltage AS next2_read_at,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        LEAD(read_at, 3) OVER (ORDER BY voltage ASC NULLS LAST, id) AS next3_read_at,
        LEAD(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at, id) AS next_voltage
    FROM scoped_readings
    WINDOW w_all_by_voltage_ties AS (ORDER BY voltage ASC NULLS LAST), w_site_id_quality_by_read_at_t AS (PARTITION BY site_id, quality ORDER BY read_at), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage NULLS LAST, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(next3_read_at) IGNORE NULLS OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next3_read_at,
        LAST_VALUE(next_voltage) IGNORE NULLS OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_voltage,
        SUM(read_at_cume_dist) OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_read_at_cume_dist,
        RANK() OVER (ORDER BY voltage_rank ASC) AS voltage_rank_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY voltage), 3) AS voltage_cume_dist_2,
        FIRST_VALUE(minutes_since_prev_read_at) IGNORE NULLS OVER (ORDER BY voltage_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_minutes_since_prev_read_at
    FROM sequenced
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    quality,
    quality_slug,
    voltage_vs_read_at_year_site_id_avg,
    voltage_cume_dist,
    voltage_rank,
    last_read_at,
    voltage_share_of_quality_site_id,
    next3_read_at,
    next_voltage,
    last_known_next3_read_at,
    last_known_next_voltage,
    voltage_rank_rank,
    voltage_cume_dist_2,
    first_known_minutes_since_prev_read_at,
    COALESCE(voltage_cume_dist_2, 0.0) AS voltage_cume_dist_2_filled,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug_2
FROM with_trends
ORDER BY voltage_cume_dist_2
