WITH open_monthly AS (
    -- OPEN rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
),
paid_monthly AS (
    -- PAID rows per claim_date_month
    SELECT DATE_TRUNC('MONTH', claim_date) AS claim_date_month, COUNT(*) AS paid_count, ROUND(AVG(claimed_amount), 2) AS paid_avg_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.claim_date_month, b.claim_date_month) AS claim_date_month,
    COALESCE(a.open_count, 0) AS open_count,
    a.open_avg_claimed_amount,
    COALESCE(b.paid_count, 0) AS paid_count,
    b.paid_avg_claimed_amount,
    COALESCE(a.open_count, 0) - COALESCE(b.paid_count, 0) AS net_count
FROM open_monthly a
FULL OUTER JOIN paid_monthly b
  ON a.claim_date_month = b.claim_date_month
ORDER BY claim_date_month
