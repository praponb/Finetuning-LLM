WITH rejected_by_adjuster AS (
    -- REJECTED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS rejected_count, SUM(claimed_amount) AS rejected_total_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
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
    COALESCE(a.rejected_count, 0) AS rejected_count,
    COALESCE(a.rejected_total_claimed_amount, 0) AS rejected_total_claimed_amount,
    COALESCE(b.paid_count, 0) AS paid_count,
    COALESCE(b.paid_total_claimed_amount, 0) AS paid_total_claimed_amount,
    COALESCE(a.rejected_count, 0) - COALESCE(b.paid_count, 0) AS rejected_count_minus_paid_count
FROM rejected_by_adjuster a
FULL OUTER JOIN paid_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
