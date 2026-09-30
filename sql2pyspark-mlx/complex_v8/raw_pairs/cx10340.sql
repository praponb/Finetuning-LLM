WITH approved_by_policy AS (
    -- APPROVED rows per policy_id
    SELECT policy_id, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY policy_id
),
open_by_policy AS (
    -- OPEN rows per policy_id
    SELECT policy_id, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY policy_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.approved_count, 0) AS approved_count,
    COALESCE(a.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    COALESCE(b.open_avg_claimed_amount, 0) AS open_avg_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.open_count, 0) AS approved_count_minus_open_count
FROM approved_by_policy a
FULL OUTER JOIN open_by_policy b
  ON a.policy_id = b.policy_id
ORDER BY policy_id
