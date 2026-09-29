WITH flattened AS (
    SELECT
        id,
        service,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
enriched AS (
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_code_avg_3,
        FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY service ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key,
        RANK() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_rank
    FROM flattened
    WHERE status_code = 422
),
latest AS (
    SELECT
        id,
        service,
        level,
        attr_key,
        v,
        status_code_avg_3,
        first_known_attr_key,
        status_code_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, level ORDER BY status_code_avg_3 DESC NULLS FIRST, id, attr_key) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
matched_keys AS (
    SELECT service, level
    FROM latest
    EXCEPT DISTINCT
    SELECT service, level
    FROM app_logs
    WHERE latency_ms BETWEEN 949 AND 1538
),
reconciled AS (
    SELECT s.*
    FROM latest s
    JOIN matched_keys k ON s.service = k.service AND s.level = k.level
),
base AS (
    SELECT
        *,
        COALESCE(status_code_avg_3, 0) AS status_code_avg_3_filled,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_clean
    FROM reconciled
    WHERE level IN ('INFO', 'DEBUG', 'ERROR')
)
SELECT
    id,
    level,
    attr_key,
    v,
    status_code_avg_3,
    first_known_attr_key,
    status_code_rank,
    CASE WHEN attr_key RLIKE '^(version|region)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS attr_key_zone
FROM base
ORDER BY status_code_avg_3 DESC
