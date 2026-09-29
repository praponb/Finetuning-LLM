WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        COUNT(DISTINCT product) AS distinct_products,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS approved_amount_to_claimed_amount_ratio,
        COUNT(*) FILTER (WHERE status IN ('APPROVED', 'PAID', 'OPEN')) AS n_status_in_approved_paid_open,
        ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount
    FROM matched
    WHERE premium < 3719.7
    GROUP BY 1
    HAVING MAX(premium) > 1042.8
)
SELECT status, n_status_in_approved_paid_open
FROM totals
