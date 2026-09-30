WITH paid_monthly AS (
    -- PAID rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS paid_count, ROUND(AVG(claimed_amount), 2) AS paid_avg_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
open_monthly AS (
    -- OPEN rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS open_count, SUM(claimed_amount) AS open_total_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.paid_count, 0) AS paid_count,
    a.paid_avg_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    b.open_total_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.open_count, 0) AS net_count
FROM paid_monthly a
FULL OUTER JOIN open_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
