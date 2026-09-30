WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(latency_ms, TRUE) OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_latency_ms,
        LAG(status_code, 1) OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key) prev_status_code
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        attr_key,
        prev_status_code,
        LEAD(latency_ms) OVER w_part next_latency_ms,
        LAG(latency_ms) OVER w_part prev_latency_ms,
        LEAD(prev_status_code) OVER w_part next_prev_status_code,
        ROW_NUMBER() OVER w_part event_seq,
        ROUND(prev_status_code / SUM(prev_status_code) OVER (PARTITION BY level), 4) prev_status_code_share
    FROM scored
    WHERE latency_ms BETWEEN 996 AND 1937
    WINDOW w_part AS (PARTITION BY level ORDER BY latency_ms, id, attr_key)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(prev_status_code AS DOUBLE) prev_status_code_num,
        FLOOR(status_code) status_code_whole,
        COALESCE(attr_key, 'N/A') attr_key_filled
    FROM metrics
    WHERE attr_key IS DISTINCT FROM 'region'
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN status_code >= 422 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
        CASE WHEN next_prev_status_code >= 439 THEN 'LARGE' ELSE 'SMALL' END AS next_prev_status_code_tier,
        SUM(status_code_whole) AS total_status_code_whole,
        COUNT_IF(attr_key = 'version') AS version_events,
        COUNT(CASE WHEN attr_key = 'region' THEN 1 END) AS region_count,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
    FROM base
    GROUP BY 1, 2
)
SELECT
    status_code_tier,
    next_prev_status_code_tier,
    total_status_code_whole,
    version_events,
    region_count
FROM stats
ORDER BY total_status_code_whole;
