WITH approved_monthly AS (
    -- APPROVED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
rejected_monthly AS (
    -- REJECTED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS rejected_count, ROUND(AVG(claimed_amount), 2) AS rejected_avg_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.approved_count, 0) AS approved_count,
    COALESCE(a.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(b.rejected_count, 0) AS rejected_count,
    COALESCE(b.rejected_avg_claimed_amount, 0) AS rejected_avg_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.rejected_count, 0) AS approved_count_minus_rejected_count
FROM approved_monthly a
FULL OUTER JOIN rejected_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
