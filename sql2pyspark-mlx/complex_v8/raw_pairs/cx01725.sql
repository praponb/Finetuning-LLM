WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE start_date >= DATE '2024-10-22'
      AND end_date < DATE '2026-06-18'
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM valid
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM valid s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        product_or_unknown,
        max_premium,
        premium_minus_max,
        max_premium - LAG(max_premium) OVER (PARTITION BY holder_id, product_or_unknown ORDER BY premium DESC, id) AS max_premium_change,
        DENSE_RANK() OVER (PARTITION BY holder_id, product_or_unknown ORDER BY premium DESC, id) AS premium_dense_rank,
        premium_minus_max - LAG(premium_minus_max) OVER w_holder_id_product_or_unknown AS premium_minus_max_change
    FROM above_average
    WINDOW w_holder_id_product_or_unknown AS (PARTITION BY holder_id, product_or_unknown ORDER BY premium DESC, id)
),
latest_per_key AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        product_or_unknown,
        max_premium,
        premium_minus_max,
        max_premium_change,
        premium_dense_rank,
        premium_minus_max_change
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY max_premium, id) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.product_or_unknown,
        s.premium_minus_max,
        s.premium_dense_rank,
        s.premium_minus_max_change,
        c.id AS claim_id,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM latest_per_key s
    JOIN claims c ON c.policy_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, measure, value
FROM (
    SELECT id, claim_id, premium_minus_max, premium_minus_max_change
    FROM detail_rows
) src
UNPIVOT (
    value FOR measure IN (premium_minus_max, premium_minus_max_change)
) unp
ORDER BY id, claim_id, measure
