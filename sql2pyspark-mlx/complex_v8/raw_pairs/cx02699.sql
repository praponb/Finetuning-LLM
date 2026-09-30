WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE start_date < DATE '2025-03-21'
      AND holder_id IS NOT NULL
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        c.id AS claim_id,
        c.claim_type,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM base_policys s
    JOIN claims c ON c.policy_id = s.id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM with_children_claims
    WHERE claim_approved_amount > (SELECT MIN(claim_approved_amount) FROM with_children_claims)
),
product_stats AS (
    -- Avg claim_approved_amount per product
    SELECT product, AVG(claim_approved_amount) AS avg_claim_approved_amount
    FROM with_activity
    GROUP BY product
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_approved_amount,
        ROUND(s.claim_approved_amount - g.avg_claim_approved_amount, 2) AS claim_approved_amount_minus_avg
    FROM with_activity s
    JOIN product_stats g ON g.product = s.product
    WHERE s.claim_approved_amount > g.avg_claim_approved_amount
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    COALESCE(product, 'ALL') AS product,
    claim_id,
    COUNT(*) AS cnt
FROM above_average
GROUP BY GROUPING SETS ((holder_id, product, claim_id), (holder_id), (product))
