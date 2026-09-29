WITH rejected_monthly AS (
    -- REJECTED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS rejected_count, SUM(claimed_amount) AS rejected_total_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
approved_monthly AS (
    -- APPROVED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.rejected_count, 0) AS rejected_count,
    a.rejected_total_claimed_amount,
    COALESCE(b.approved_count, 0) AS approved_count,
    COALESCE(b.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(a.rejected_count, 0) - COALESCE(b.approved_count, 0) AS rejected_count_minus_approved_count
FROM rejected_monthly a
FULL OUTER JOIN approved_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
