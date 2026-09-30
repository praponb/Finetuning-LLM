WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE site_id <= 6
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        quality_len,
        kwh_bucket,
        LEAD(read_at, 2) OVER w_all_by_read_at AS next2_read_at,
        LEAD(quality_len) OVER (ORDER BY quality_len ASC, id) AS next_quality_len,
        kwh_bucket - LAG(kwh_bucket) OVER (ORDER BY quality_len ASC, id) AS kwh_bucket_change,
        kwh - LAG(kwh) OVER (ORDER BY quality_len ASC, id) AS kwh_change
    FROM scoped
    WINDOW w_all_by_read_at AS (ORDER BY read_at ASC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        quality_len,
        next_quality_len,
        kwh_bucket_change,
        FLOOR(kwh_bucket / 25) * 25 AS kwh_bucket_bucket,
        DATE_TRUNC('HOUR', next2_read_at) AS next2_read_at_hour,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM sequenced
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE quality_len > (SELECT AVG(quality_len) FROM tagged)
)
SELECT
    next_quality_len,
    read_at_dayofweek,
    ROUND(AVG(next_quality_len), 2) AS avg_next_quality_len,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(quality_len, 0.5) AS median_quality_len
FROM filtered
GROUP BY ROLLUP(next_quality_len, read_at_dayofweek)
ORDER BY next_quality_len NULLS LAST, read_at_dayofweek NULLS LAST
