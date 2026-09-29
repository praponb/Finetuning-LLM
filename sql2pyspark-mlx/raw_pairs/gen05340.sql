WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        k,
        v
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id, k) AS rank_in_group
        FROM flattened
    ) t
    WHERE rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(k) AS k_lower,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM ranked
    WHERE k = 'trace'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(k, 1, 4) AS k_prefix,
        UPPER(k) AS k_upper,
        COALESCE(k, 'NONE') AS resolved_k
    FROM filtered
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'trace'
)
SELECT
    id,
    k,
    v,
    k_clean,
    k_prefix,
    k_upper,
    resolved_k
FROM base
ORDER BY k;
