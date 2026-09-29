WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE (site_id <= 4 OR quality IS NOT NULL)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        kwh_filled - LAG(kwh_filled) OVER (PARTITION BY site_id ORDER BY kwh_filled ASC, id) AS kwh_filled_change,
        FIRST_VALUE(kwh) OVER (ORDER BY quality_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality_len)) AS distinct_site_id_in_quality_len,
        ROUND(PERCENT_RANK() OVER w_site_id_by_kwh_filled, 3) AS kwh_filled_percent_rank,
        COUNT(*) OVER w_quality_len AS rows_in_quality_len,
        LEAD(quality_len) OVER (PARTITION BY site_id, quality_len ORDER BY read_at DESC, id) AS next_quality_len,
        DENSE_RANK() OVER (PARTITION BY site_id, quality_len ORDER BY read_at DESC) AS read_at_dense_rank,
        LEAD(kwh) OVER w_site_id_quality_len_by_read AS next_kwh
    FROM recent_readings
    WINDOW w_site_id_by_kwh_filled AS (PARTITION BY site_id ORDER BY kwh_filled ASC, id), w_quality_len AS (PARTITION BY quality_len), w_site_id_quality_len_by_read AS (PARTITION BY site_id, quality_len ORDER BY read_at DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    kwh_filled,
    quality_len,
    distinct_site_id_in_quality_len,
    rows_in_quality_len,
    next_quality_len,
    next_kwh,
    CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
    FLOOR(next_quality_len / 2) * 2 AS next_quality_len_bucket
FROM sequenced
WHERE kwh_filled < 66.0
ORDER BY quality
