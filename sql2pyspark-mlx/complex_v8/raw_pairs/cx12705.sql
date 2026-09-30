WITH paid_by_adjuster AS (
    -- PAID rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS paid_count, ROUND(AVG(claimed_amount), 2) AS paid_avg_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY adjuster_id
),
approved_by_adjuster AS (
    -- APPROVED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS approved_count, ROUND(AVG(claimed_amount), 2) AS approved_avg_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY adjuster_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.paid_count, 0) AS paid_count,
    a.paid_avg_claimed_amount,
    COALESCE(b.approved_count, 0) AS approved_count,
    b.approved_avg_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.approved_count, 0) AS paid_count_minus_approved_count
FROM paid_by_adjuster a
FULL OUTER JOIN approved_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
