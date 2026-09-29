WITH paid_monthly AS (
    -- PAID rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
rejected_monthly AS (
    -- REJECTED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS rejected_count, SUM(claimed_amount) AS rejected_total_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.paid_count, 0) AS paid_count,
    a.paid_total_claimed_amount,
    COALESCE(b.rejected_count, 0) AS rejected_count,
    b.rejected_total_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.rejected_count, 0) AS net_count
FROM paid_monthly a
FULL OUTER JOIN rejected_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
