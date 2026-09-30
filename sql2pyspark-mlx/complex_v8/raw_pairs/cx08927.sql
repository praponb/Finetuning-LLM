WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d,
        COALESCE(premium, 0.0) AS premium_filled
    FROM matched
    WHERE approved_amount > 4868.7
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        ROUND(AVG(premium), 2) AS avg_premium,
        COUNT(DISTINCT id) AS distinct_ids,
        DATEDIFF(MAX(claim_date_plus_365d), MIN(claim_date_plus_365d)) AS claim_date_plus_365d_span_days,
        SUM(claimed_amount) AS total_claimed_amount
    FROM labeled
    WHERE premium_filled <= 3800.0
    GROUP BY 1
)
SELECT
    claim_date_ym,
    avg_premium,
    distinct_ids,
    claim_date_plus_365d_span_days,
    COALESCE(avg_premium, 0.0) AS avg_premium_filled,
    LENGTH(TRIM(claim_date_ym)) AS claim_date_ym_len
FROM grouped
ORDER BY avg_premium DESC NULLS FIRST, claim_date_ym_len DESC NULLS FIRST
