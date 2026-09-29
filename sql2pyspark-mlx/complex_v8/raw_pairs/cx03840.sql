WITH rejected_by_adjuster AS (
    -- REJECTED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS rejected_count, SUM(claimed_amount) AS rejected_total_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY adjuster_id
),
approved_by_adjuster AS (
    -- APPROVED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY adjuster_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.rejected_count, 0) AS rejected_count,
    COALESCE(a.rejected_total_claimed_amount, 0) AS rejected_total_claimed_amount,
    COALESCE(b.approved_count, 0) AS approved_count,
    b.approved_total_claimed_amount,
    COALESCE(a.rejected_count, 0) - COALESCE(b.approved_count, 0) AS rejected_count_minus_approved_count
FROM rejected_by_adjuster a
FULL OUTER JOIN approved_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
