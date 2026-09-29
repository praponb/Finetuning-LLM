WITH approved_monthly AS (
    -- APPROVED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS approved_count, ROUND(AVG(claimed_amount), 2) AS approved_avg_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
paid_monthly AS (
    -- PAID rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.approved_count, 0) AS approved_count,
    a.approved_avg_claimed_amount,
    COALESCE(b.paid_count, 0) AS paid_count,
    b.paid_total_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.paid_count, 0) AS approved_count_minus_paid_count
FROM approved_monthly a
FULL OUTER JOIN paid_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
