WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
grouped AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        ROUND(SUM(premium) / NULLIF(SUM(claimed_amount), 0), 3) AS premium_to_claimed_amount_ratio,
        MAX(claimed_amount) AS max_claimed_amount
    FROM matched
    GROUP BY adjuster_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        max_claimed_amount - avg_claimed_amount AS max_claimed_amount_minus_avg_claimed_amount,
        ROUND(avg_claimed_amount * 1.07, 2) AS avg_claimed_amount_adj,
        GREATEST(max_claimed_amount, avg_claimed_amount) AS greatest_max_claimed_amount_avg_claimed_amount
    FROM grouped
    WHERE premium_to_claimed_amount_ratio <= 8.5
)
SELECT
    adjuster_id,
    avg_claimed_amount,
    premium_to_claimed_amount_ratio,
    max_claimed_amount,
    avg_claimed_amount_adj,
    greatest_max_claimed_amount_avg_claimed_amount
FROM tagged
ORDER BY premium_to_claimed_amount_ratio DESC
