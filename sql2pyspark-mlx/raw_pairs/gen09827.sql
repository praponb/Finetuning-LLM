WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        k,
        v
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k, customer_id ORDER BY created_at, id, k) AS dedup_rank
        FROM lines
    ) t
    WHERE dedup_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(k) AS k_length
    FROM deduped
    WHERE k = 'utm_source'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY k_length DESC NULLS LAST, id, k) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    k,
    k_length,
    dedup_rank,
    CASE WHEN k RLIKE '^(utm_source|weight_kg)$' THEN 'OTHER_ZONE' WHEN k RLIKE '^(device|gift)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS k_zone
FROM ranked
ORDER BY k DESC;
