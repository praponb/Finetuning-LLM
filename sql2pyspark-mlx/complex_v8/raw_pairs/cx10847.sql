WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh > 45.0
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        ROUND(AVG(kwh) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_ties, 3) AS read_at_percent_rank,
        RANK() OVER w_all_by_read_at_ties AS read_at_rank,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh, id), 3) AS kwh_percent_rank,
        DENSE_RANK() OVER w_all_by_kwh AS kwh_dense_rank,
        SIZE(COLLECT_SET(quality) OVER w_site_id) AS distinct_quality_in_site_id,
        ROUND(CUME_DIST() OVER (ORDER BY read_at DESC NULLS FIRST), 3) AS read_at_cume_dist,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id), 2) AS site_id_avg_kwh,
        ROW_NUMBER() OVER (ORDER BY kwh, id) AS kwh_seq,
        ROW_NUMBER() OVER (ORDER BY read_at DESC NULLS FIRST, id) AS rn
    FROM base
    WINDOW w_all_by_read_at_ties AS (ORDER BY read_at DESC NULLS FIRST), w_all_by_kwh AS (ORDER BY kwh, id), w_site_id AS (PARTITION BY site_id)
)
SELECT
    id,
    site_id,
    kwh,
    kwh_avg_3,
    read_at_percent_rank,
    last_known_kwh,
    kwh_percent_rank,
    kwh_dense_rank,
    distinct_quality_in_site_id,
    read_at_cume_dist,
    site_id_avg_kwh,
    kwh_seq,
    rn,
    ABS(last_known_kwh - 58.5) AS last_known_kwh_distance,
    FLOOR(site_id_avg_kwh / 25) * 25 AS site_id_avg_kwh_bucket
FROM windowed
ORDER BY kwh_seq, id
LIMIT 5
