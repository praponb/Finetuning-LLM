WITH open_by_policy AS (
    -- OPEN rows per policy_id
    SELECT policy_id, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY policy_id
),
rejected_by_policy AS (
    -- REJECTED rows per policy_id
    SELECT policy_id, COUNT(*) AS rejected_count, ROUND(AVG(claimed_amount), 2) AS rejected_avg_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
    GROUP BY policy_id
)
SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.open_count, 0) AS open_count,
    a.open_avg_claimed_amount,
    COALESCE(b.rejected_count, 0) AS rejected_count,
    b.rejected_avg_claimed_amount,
    COALESCE(a.open_count, 0) - COALESCE(b.rejected_count, 0) AS net_count
FROM open_by_policy a
FULL OUTER JOIN rejected_by_policy b
  ON a.policy_id = b.policy_id
ORDER BY policy_id
