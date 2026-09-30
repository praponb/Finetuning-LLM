WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage > 214.3
      AND site_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        quality,
        quality_slug,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_max_4,
        ROW_NUMBER() OVER (ORDER BY kwh DESC, id) AS seq_no,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id, quality), 0), 4) AS kwh_share_of_site_id_quality,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank,
        ROUND(AVG(kwh) OVER w_site_id_quality, 2) AS site_id_quality_avg_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY read_at, id), 3) AS read_at_cume_dist,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        SUM(voltage) OVER (PARTITION BY quality) AS quality_sum_voltage,
        MAX(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        LAG(read_at, 3) OVER (PARTITION BY site_id ORDER BY kwh ASC, id) AS prev3_read_at
    FROM filtered
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh DESC), w_site_id_quality AS (PARTITION BY site_id, quality)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE seq_no = 1
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    quality_slug,
    voltage_max_4,
    seq_no,
    second_read_at,
    last_known_read_at,
    kwh_share_of_site_id_quality,
    kwh_rank,
    site_id_quality_avg_kwh,
    kwh_share_of_quality,
    voltage_max_3
FROM top_rows
ORDER BY site_id, voltage_max_4 DESC
