WITH matched AS (
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
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        status,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(premium) AS min_premium,
        MIN(claimed_amount) AS min_claimed_amount,
        SUM(premium) AS total_premium,
        COUNT(DISTINCT status) AS distinct_status
    FROM matched
    GROUP BY DATE_TRUNC('MONTH', claim_date), status
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM rollup_base)
)
SELECT claim_date_month, status, distinct_status
FROM qualified
ORDER BY status, claim_date_month
LIMIT 20
