WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        ROUND(premium * 0.9, 2) AS premium_adj
    FROM policies
    WHERE end_date IS NOT NULL
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date >= DATE '2024-08-07'
        )
      AND s.product_slug IS NOT NULL
),
latest AS (
    -- Keep the earliest row per product
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        product_slug,
        premium_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium_adj, id) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.premium_adj,
        p.joined_on AS holder_joined_on,
        p.holder_name
    FROM latest s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2024-12-21'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.premium_adj,
        s.holder_joined_on,
        s.holder_name,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type
    FROM with_details s
    JOIN claims c ON c.policy_id = s.id
),
latest_per_key AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        premium_adj,
        holder_joined_on,
        holder_name,
        claim_id,
        claim_adjuster_id,
        claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY end_date DESC, id DESC, claim_id DESC) AS recency_rank
        FROM with_children
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    product,
    end_date,
    holder_joined_on,
    holder_name,
    claim_adjuster_id,
    claim_type,
    ABS(premium_adj - 4945.0) AS premium_adj_distance,
    COALESCE(premium_adj, 0.0) AS premium_adj_filled
FROM latest_per_key
ORDER BY end_date DESC, holder_joined_on
