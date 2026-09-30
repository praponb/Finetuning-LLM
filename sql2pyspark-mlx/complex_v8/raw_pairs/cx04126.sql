WITH approved_by_adjuster AS (
    -- APPROVED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY adjuster_id
),
paid_by_adjuster AS (
    -- PAID rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY adjuster_id
)
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.approved_count, 0) AS approved_count,
    COALESCE(a.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(b.paid_count, 0) AS paid_count,
    b.paid_total_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.paid_count, 0) AS approved_count_minus_paid_count
FROM approved_by_adjuster a
FULL OUTER JOIN paid_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
