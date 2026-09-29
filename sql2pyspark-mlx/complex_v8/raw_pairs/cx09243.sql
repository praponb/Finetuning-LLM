WITH open_by_adjuster AS (
    -- OPEN rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS open_count, SUM(claimed_amount) AS open_total_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY adjuster_id
),
paid_by_adjuster AS (
    -- PAID rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY adjuster_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.open_count, 0) AS open_count,
    COALESCE(a.open_total_claimed_amount, 0) AS open_total_claimed_amount,
    COALESCE(b.paid_count, 0) AS paid_count,
    b.paid_total_claimed_amount,
    COALESCE(a.open_count, 0) - COALESCE(b.paid_count, 0) AS open_count_minus_paid_count
FROM open_by_adjuster a
FULL OUTER JOIN paid_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
