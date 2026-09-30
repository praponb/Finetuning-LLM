WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date
    FROM policies
    WHERE product <> 'TRAVEL'
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.holder_name IS NOT NULL
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claim_date) AS last_claim_date,
        MIN(approved_amount) AS min_claim_approved_amount,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        c.min_claim_approved_amount,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM filtered s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        ROUND(SUM(premium), 2) AS total_premium,
        COUNT(*) FILTER (WHERE product <> 'AUTO') AS n_product_auto
    FROM with_counts
    GROUP BY product
    HAVING SUM(claim_count) > 3
)
-- Final projection
SELECT product, total_premium
FROM stats
WHERE n_product_auto >= 2
ORDER BY product DESC NULLS FIRST
