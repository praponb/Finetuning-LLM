WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        value,
        SUBSTRING(host, 1, 3) AS host_prefix
    FROM flattened
    WHERE log_ts < '2024-01-01'
      AND attr_key IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts), attr_key ORDER BY status_code, id, attr_key) AS event_seq,
        AVG(status_code) OVER (PARTITION BY attr_key, level ORDER BY status_code, id, attr_key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY status_code DESC, id, attr_key) AS row_seq,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY YEAR(log_ts), attr_key), 4) AS status_code_share
    FROM cleaned
    WHERE status_code > 283
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY log_ts DESC, id, attr_key) = 1
),
deduped_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY event_seq NULLS FIRST, id, attr_key) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN status_code_avg_6_prior >= 422 THEN 'LARGE' ELSE 'SMALL' END AS status_code_avg_6_prior_tier,
        attr_key,
        CASE WHEN status_code_share >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_tier,
        COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        MIN(log_ts) AS first_seen,
        ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN level = 'WARN' THEN 1 END), 0), 2) AS status_code_per_warn,
        COUNT(DISTINCT level) AS distinct_levels
    FROM deduped_app_logs
    GROUP BY CASE WHEN status_code_avg_6_prior >= 422 THEN 'LARGE' ELSE 'SMALL' END, attr_key, CASE WHEN status_code_share >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    status_code_avg_6_prior_tier,
    attr_key,
    status_code_share_tier,
    warn_count,
    first_seen,
    status_code_per_warn,
    distinct_levels
FROM grouped
ORDER BY status_code_avg_6_prior_tier;
