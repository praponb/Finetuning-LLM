WITH paid_by_policy AS (
    -- PAID rows per policy_id
    SELECT policy_id, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY policy_id
),
open_by_policy AS (
    -- OPEN rows per policy_id
    SELECT policy_id, COUNT(*) AS open_count, ROUND(AVG(claimed_amount), 2) AS open_avg_claimed_amount
    FROM claims
    WHERE status = 'OPEN'
    GROUP BY policy_id
)
SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.paid_count, 0) AS paid_count,
    a.paid_total_claimed_amount,
    COALESCE(b.open_count, 0) AS open_count,
    b.open_avg_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.open_count, 0) AS net_count
FROM paid_by_policy a
FULL OUTER JOIN open_by_policy b
  ON a.policy_id = b.policy_id
ORDER BY policy_id
