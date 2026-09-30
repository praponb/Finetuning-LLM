WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        log_ts,
        attr_key,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM lines
    WHERE attr_key IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY host ORDER BY days_since_log_ts) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
ranked_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latest_rank, id, attr_key) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(host, ' ', service) AS host_service
    FROM ranked_app_logs
    WHERE log_ts >= DATE '2025-06-01'
)
SELECT
    id,
    log_ts,
    attr_key,
    days_since_log_ts,
    rn,
    CASE WHEN attr_key RLIKE '^(trace|region)$' THEN 'GROUP_B' WHEN attr_key RLIKE '^(version)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS attr_key_zone
FROM base
ORDER BY attr_key_zone DESC, days_since_log_ts DESC
