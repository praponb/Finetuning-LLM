WITH rejected_by_policy AS (
    -- REJECTED rows per policy_id
    SELECT policy_id, COUNT(*) AS rejected_count, SUM(claimed_amount) AS rejected_total_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY policy_id
),
open_by_policy AS (
    -- OPEN rows per policy_id
    SELECT policy_id, COUNT(*) AS open_count, SUM(claimed_amount) AS open_total_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY policy_id
)
SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.rejected_count, 0) AS rejected_count,
    COALESCE(a.rejected_total_claimed_amount, 0) AS rejected_total_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    b.open_total_claimed_amount,
    COALESCE(a.rejected_count, 0) - COALESCE(b.open_count, 0) AS net_count
FROM rejected_by_policy a
FULL OUTER JOIN open_by_policy b
  ON a.policy_id = b.policy_id
ORDER BY policy_id
