WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(level, ', ', k) AS level_k,
        INITCAP(TRIM(level)) AS level_title
    FROM lines
    WHERE k IS NOT NULL
      AND k IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        level,
        k,
        value,
        level_k,
        level_title,
        CASE WHEN level = 'WARN' THEN 1 ELSE 0 END AS is_warn,
        CONCAT_WS('-', k, level) AS k_level_key
    FROM filtered
    WHERE k IN ('version', 'trace')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY is_warn NULLS LAST) AS is_warn_dense_rank,
        RANK() OVER (PARTITION BY k ORDER BY is_warn NULLS LAST) AS is_warn_rank,
        MIN(is_warn) OVER (PARTITION BY level) AS level_min_is_warn
    FROM valid
    WHERE level NOT IN ('DEBUG', 'INFO')
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        level,
        value,
        level_k,
        is_warn,
        k_level_key,
        level_min_is_warn,
        COALESCE(k, 'Unknown') AS k_filled,
        COALESCE(k, 'N/A') AS k_clean
    FROM scored
    WHERE is_warn_dense_rank = 4
)
SELECT
    level,
    value,
    level_k,
    k_level_key,
    k_filled
FROM filtered_app_logs
ORDER BY level DESC;
