WITH open_by_adjuster AS (
    -- OPEN rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY adjuster_id
),
rejected_by_adjuster AS (
    -- REJECTED rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS rejected_count, SUM(claimed_amount) AS rejected_total_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY adjuster_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.open_count, 0) AS open_count,
    a.open_avg_claimed_amount,
    COALESCE(b.rejected_count, 0) AS rejected_count,
    b.rejected_total_claimed_amount,
    COALESCE(a.open_count, 0) - COALESCE(b.rejected_count, 0) AS net_count
FROM open_by_adjuster a
FULL OUTER JOIN rejected_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
