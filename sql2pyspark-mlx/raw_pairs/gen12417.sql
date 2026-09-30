WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        value,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM lines
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(tag) AS tag_length
    FROM lines_carts
    WHERE tag = 'SPRING10'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(tag, 'unassigned') AS tag_filled
    FROM valid
    WHERE tag IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_length DESC NULLS LAST) AS tag_length_dense_rank,
        SUM(tag_length) OVER (PARTITION BY tag) AS tag_sum_tag_length,
        RANK() OVER (ORDER BY tag_length DESC NULLS FIRST) AS tag_length_rank
    FROM base
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE customer_id = 22
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        value,
        tag,
        tag_filled,
        tag_length_dense_rank,
        tag_sum_tag_length,
        tag_length_rank,
        LENGTH(tag) AS tag_length_2
    FROM reconciled
    WHERE tag IS NOT NULL
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        tag,
        ROUND(SUM(tag_length_dense_rank) / NULLIF(COUNT(CASE WHEN tag = 'BUNDLE' THEN 1 END), 0), 2) AS tag_length_dense_rank_per_bundle,
        COUNT(CASE WHEN tag = 'VIP' THEN 1 END) AS vip_count
    FROM valid_carts
    GROUP BY 1
    HAVING tag_length_dense_rank_per_bundle > 0
)
SELECT
    tag,
    tag_length_dense_rank_per_bundle,
    vip_count
FROM summary
ORDER BY tag_length_dense_rank_per_bundle DESC
