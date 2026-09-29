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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.status,
        s.premium,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM covered s
    JOIN claims c ON c.policy_id = s.policy_id
),
grouped AS (
    -- Group-level totals
    SELECT
        claim_id,
        ROUND(SUM(claimed_amount), 2) AS total_claimed_amount,
        ROUND(SUM(claimed_amount) / NULLIF(SUM(premium), 0), 3) AS claimed_amount_to_premium_ratio
    FROM line_level
    GROUP BY claim_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_id) AS rows_in_claim_id,
        NTILE(10) OVER (PARTITION BY claim_id ORDER BY claimed_amount_to_premium_ratio NULLS LAST, claim_id) AS claimed_amount_to_premium_ratio_decile,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount_to_premium_ratio), 3) AS claimed_amount_to_premium_ratio_percent_rank,
        MAX(total_claimed_amount) OVER (PARTITION BY claim_id ORDER BY claimed_amount_to_premium_ratio NULLS LAST, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_total_claimed_amount_so_far
    FROM grouped
)
-- Final projection
SELECT
    claim_id,
    total_claimed_amount,
    claimed_amount_to_premium_ratio,
    rows_in_claim_id,
    claimed_amount_to_premium_ratio_decile,
    max_total_claimed_amount_so_far
FROM with_windows
WHERE claimed_amount_to_premium_ratio_percent_rank BETWEEN 0.1 AND 0.6
ORDER BY claimed_amount_to_premium_ratio, max_total_claimed_amount_so_far DESC
