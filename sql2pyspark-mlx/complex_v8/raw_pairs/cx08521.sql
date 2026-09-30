WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 1541.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE premium >= 2096.6
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.premium_filled,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM scoped_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
)
SELECT
    COALESCE(claim_status, 'ALL') AS claim_status,
    COALESCE(product, 'ALL') AS product,
    GROUPING(claim_status) AS is_total_claim_status,
    ROUND(100 * COUNT(CASE WHEN claim_status = 'OPEN' THEN 1 END) / COUNT(*), 1) AS pct_claim_status_open,
    SUM(claim_approved_amount) FILTER (WHERE claim_status IN ('APPROVED', 'OPEN')) AS claim_approved_amount_claim_status_in_approved_o
FROM line_level
GROUP BY GROUPING SETS ((claim_status, product), (claim_status))
ORDER BY claim_status NULLS LAST, product NULLS LAST
