WITH approved_by_policy AS (
    -- APPROVED rows per policy_id
    SELECT policy_id, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY policy_id
),
paid_by_policy AS (
    -- PAID rows per policy_id
    SELECT policy_id, COUNT(*) AS paid_count, SUM(claimed_amount) AS paid_total_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY policy_id
)
SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.approved_count, 0) AS approved_count,
    a.approved_total_claimed_amount,
    COALESCE(b.paid_count, 0) AS paid_count,
    b.paid_total_claimed_amount,
    COALESCE(a.approved_count, 0) - COALESCE(b.paid_count, 0) AS approved_count_minus_paid_count
FROM approved_by_policy a
FULL OUTER JOIN paid_by_policy b
  ON a.policy_id = b.policy_id
ORDER BY policy_id
