WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE end_date >= DATE '2025-12-05'
      AND product = 'HEALTH'
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_claimed_amount,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_approved_amount
    FROM valid_policys s
)
SELECT
    holder_id,
    COALESCE(status, 'ALL') AS status,
    MAX(end_date) AS last_end_date,
    ROUND(SUM(premium), 2) AS total_premium,
    ROUND(100 * COUNT(CASE WHEN product = 'HEALTH' THEN 1 END) / COUNT(*), 1) AS pct_product_health
FROM summarized_rows
GROUP BY GROUPING SETS ((holder_id, status), (holder_id), ())
ORDER BY holder_id NULLS LAST, status NULLS LAST
