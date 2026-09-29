WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(key)) key_title,
        COALESCE(key, 'DIRECT') key_clean
    FROM exploded
    WHERE key IS DISTINCT FROM 'region'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        v,
        COUNT(latency_ms) OVER (PARTITION BY key) key_count_latency_ms
    FROM cleaned
    WHERE latency_ms <= 1401
),
scored AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER w_running key_count_latency_ms_percent_rank
    FROM enriched
    WINDOW w_running AS (ORDER BY key_count_latency_ms NULLS FIRST)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE latency_ms <> 1363
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM reconciled
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE latency_ms BETWEEN 732 AND 1746
),
reconciled_app_logs AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        latency_ms,
        v,
        key_count_latency_ms,
        key_count_latency_ms_percent_rank,
        key_count_latency_ms_percent_rank - latency_ms key_count_latency_ms_percent_rank_minus_latency_ms
    FROM reconciled_app_logs
    WHERE key_count_latency_ms <= 16
)
SELECT
    latency_ms,
    v,
    key_count_latency_ms,
    key_count_latency_ms_percent_rank,
    key_count_latency_ms_percent_rank_minus_latency_ms
FROM base
