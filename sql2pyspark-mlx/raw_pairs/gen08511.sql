WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        log_ts,
        key,
        LAST_VALUE(key, TRUE) OVER w_ordered AS last_known_key
    FROM lines
    WINDOW w_ordered AS (PARTITION BY MONTH(log_ts), key ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        key,
        last_known_key,
        CAST(log_ts AS DATE) AS log_ts_day
    FROM enriched
    WHERE host = 'h1'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts_day ASC, id ASC, key ASC) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    key,
    DATE_TRUNC('QUARTER', log_ts_day) AS quarter_start,
    COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count_2,
    DENSE_RANK() OVER (PARTITION BY key ORDER BY COUNT(*) FILTER (WHERE key = 'version') DESC) AS version_rows_dense_rank
FROM ranked
GROUP BY key, quarter_start
