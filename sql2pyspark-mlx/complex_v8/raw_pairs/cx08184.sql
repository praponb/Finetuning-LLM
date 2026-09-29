WITH approved_monthly AS (
    -- APPROVED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
open_monthly AS (
    -- OPEN rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.approved_count, 0) AS approved_count,
    COALESCE(a.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    b.open_avg_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.open_count, 0) AS net_count
FROM approved_monthly a
FULL OUTER JOIN open_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
