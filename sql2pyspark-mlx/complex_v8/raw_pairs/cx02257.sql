WITH rejected_monthly AS (
    -- REJECTED rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS rejected_count, ROUND(AVG(claimed_amount), 2) AS rejected_avg_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
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
    COALESCE(a.rejected_count, 0) AS rejected_count,
    a.rejected_avg_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    COALESCE(b.open_avg_claimed_amount, 0) AS open_avg_claimed_amount,
    COALESCE(a.rejected_count, 0) - COALESCE(b.open_count, 0) AS net_count
FROM rejected_monthly a
FULL OUTER JOIN open_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
