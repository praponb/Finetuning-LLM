WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'EST')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY kwh ASC), 3) AS kwh_cume_dist,
        LEAD(read_at) OVER (ORDER BY kwh ASC, id) AS next_read_at
    FROM scoped
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(kwh_bucket) OVER (PARTITION BY site_id ORDER BY kwh_cume_dist DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_bucket_max_3,
        LAG(kwh_cume_dist, 1, 0) OVER (PARTITION BY site_id ORDER BY kwh_cume_dist DESC NULLS FIRST, id) AS prev_kwh_cume_dist
    FROM sequenced
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    quality_ref,
    kwh_bucket,
    read_at_label,
    kwh_cume_dist,
    kwh_bucket_max_3,
    prev_kwh_cume_dist
FROM ranked
ORDER BY kwh_bucket_max_3 DESC NULLS FIRST, kwh DESC
