WITH paid_by_adjuster AS (
    -- PAID rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY adjuster_id
),
rejected_by_adjuster AS (
    -- REJECTED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS rejected_count, ROUND(AVG(claimed_amount), 2) AS rejected_avg_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY adjuster_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.paid_count, 0) AS paid_count,
    COALESCE(a.paid_total_claimed_amount, 0) AS paid_total_claimed_amount,
    COALESCE(b.rejected_count, 0) AS rejected_count,
    b.rejected_avg_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.rejected_count, 0) AS net_count
FROM paid_by_adjuster a
FULL OUTER JOIN rejected_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
