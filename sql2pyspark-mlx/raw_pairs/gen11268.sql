WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code, id, attr_key) AS rn
        FROM exploded
    ) x
    WHERE rn = 1
),
kept_keys AS (
    SELECT service
    FROM ranked
    INTERSECT
    SELECT service
    FROM app_logs
    WHERE log_ts >= '2025-08-04'
),
kept_rows AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.service = k.service
),
base AS (
    SELECT *, CONCAT_WS(',', attr_key, level) AS attr_key_level_key
    FROM kept_rows
    WHERE log_ts > DATE '2025-06-01'
),
base_app_logs AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), log_ts) / 30 AS months_since_log_ts
    FROM base
    WHERE log_ts > DATE '2025-06-01'
      AND level NOT IN ('ERROR', 'DEBUG', 'WARN')
)
SELECT
    id,
    log_ts,
    level,
    attr_key,
    attr_value,
    attr_key_level_key,
    CASE WHEN level RLIKE '^(WARN|INFO)$' THEN 'CORE' WHEN level RLIKE '^(ERROR|DEBUG)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS level_zone
FROM base_app_logs
ORDER BY log_ts DESC NULLS LAST
