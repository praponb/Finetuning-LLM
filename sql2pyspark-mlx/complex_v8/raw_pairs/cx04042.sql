WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
scored AS (
    -- Add calculated columns
    SELECT *, ROUND(premium * 1.1, 2) AS premium_adj
    FROM covered
    WHERE claim_date >= DATE '2024-08-18'
),
product_stats AS (
    -- Max premium_adj per product
    SELECT product, MAX(premium_adj) AS max_premium_adj
    FROM scored
    GROUP BY product
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium_adj,
        ROUND(s.premium_adj - g.max_premium_adj, 2) AS premium_adj_minus_max
    FROM scored s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium_adj = g.max_premium_adj
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    claim_type,
    premium_adj,
    premium_adj_minus_max,
    LENGTH(TRIM(status)) AS status_len,
    CASE WHEN premium_adj >= 5310.0 THEN 'A' WHEN premium_adj >= 2025.0 THEN 'B' ELSE 'C' END AS premium_adj_tier
FROM compared
WHERE premium BETWEEN 986.9 AND 4688.7
