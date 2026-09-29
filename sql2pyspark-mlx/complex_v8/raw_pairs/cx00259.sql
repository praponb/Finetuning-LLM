WITH paid_by_adjuster AS (
    -- PAID rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY adjuster_id
),
open_by_adjuster AS (
    -- OPEN rows per adjuster_id
    SELECT adjuster_id, COUNT(*) AS open_count, SUM(claimed_amount) AS open_total_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY adjuster_id
)
SELECT
    COALESCE(a.adjuster_id, b.adjuster_id) AS adjuster_id,
    COALESCE(a.paid_count, 0) AS paid_count,
    COALESCE(a.paid_total_claimed_amount, 0) AS paid_total_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    COALESCE(b.open_total_claimed_amount, 0) AS open_total_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.open_count, 0) AS net_count
FROM paid_by_adjuster a
FULL OUTER JOIN open_by_adjuster b
  ON a.adjuster_id = b.adjuster_id
ORDER BY adjuster_id
