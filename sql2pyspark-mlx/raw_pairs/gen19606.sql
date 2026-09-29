WITH flattened AS (
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
scored AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY status_code) AS status_code_dense_rank
    FROM flattened
),
base AS (
    SELECT *, INITCAP(TRIM(level)) AS level_title
    FROM scored
    WHERE status_code != 389
),
filtered AS (
    SELECT
        id,
        level,
        key,
        ROUND(status_code / status_code_dense_rank, 2) AS status_code_per_status_code_dense_rank,
        COALESCE(key, 'DIRECT') AS key_clean
    FROM base
    WHERE key IS NOT NULL
      AND service IN ('auth', 'billing', 'checkout')
),
metrics AS (
    SELECT
        level,
        key,
        status_code_per_status_code_dense_rank,
        RANK() OVER (PARTITION BY level, key ORDER BY status_code_per_status_code_dense_rank) AS status_code_per_status_code_dense_rank_rank
    FROM filtered
),
enriched AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY status_code_per_status_code_dense_rank NULLS LAST) AS status_code_per_status_code_dense_rank_dense_rank
    FROM metrics
    WHERE status_code_per_status_code_dense_rank > 76
)
SELECT
    CASE WHEN status_code_per_status_code_dense_rank >= 81 THEN 'LARGE' ELSE 'SMALL' END AS status_code_per_status_code_dense_rank_tier,
    COUNT_IF(level = 'INFO') AS info_events,
    COUNT(DISTINCT key) AS distinct_keys
FROM enriched
GROUP BY CUBE (CASE WHEN status_code_per_status_code_dense_rank >= 81 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY info_events DESC NULLS FIRST;
