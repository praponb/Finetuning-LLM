WITH open_by_adjuster AS (
    -- OPEN rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
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
    COALESCE(a.open_count, 0) AS open_count,
    COALESCE(a.open_avg_claimed_amount, 0) AS open_avg_claimed_amount,
    COALESCE(b.approved_count, 0) AS approved_count,
    b.approved_avg_claimed_amount,
    COALESCE(a.open_count, 0) - COALESCE(b.approved_count, 0) AS net_count
FROM open_by_adjuster a
FULL OUTER JOIN approved_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
