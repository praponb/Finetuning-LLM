WITH approved_by_adjuster AS (
    -- APPROVED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY adjuster_id
),
rejected_by_adjuster AS (
    -- REJECTED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS rejected_count, ROUND(AVG(claimed_amount), 2) AS rejected_avg_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY adjuster_id
)
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.approved_count, 0) AS approved_count,
    COALESCE(a.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(b.rejected_count, 0) AS rejected_count,
    b.rejected_avg_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.rejected_count, 0) AS approved_count_minus_rejected_count
FROM approved_by_adjuster a
FULL OUTER JOIN rejected_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
