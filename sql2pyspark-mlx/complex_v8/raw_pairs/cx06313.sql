WITH paid_by_policy AS (
    -- PAID rows per policy_id
    SELECT policy_id, COUNT(*) AS paid_count, ROUND(AVG(claimed_amount), 2) AS paid_avg_claimed_amount
    FROM claims
    WHERE status = 'PAID'
    GROUP BY policy_id
),
approved_by_policy AS (
    -- APPROVED rows per policy_id
    SELECT policy_id, COUNT(*) AS approved_count, SUM(claimed_amount) AS approved_total_claimed_amount
    FROM claims
    WHERE status = 'APPROVED'
    GROUP BY policy_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.paid_count, 0) AS paid_count,
    COALESCE(a.paid_avg_claimed_amount, 0) AS paid_avg_claimed_amount,
    COALESCE(b.approved_count, 0) AS approved_count,
    COALESCE(b.approved_total_claimed_amount, 0) AS approved_total_claimed_amount,
    COALESCE(a.paid_count, 0) - COALESCE(b.approved_count, 0) AS net_count
FROM paid_by_policy a
FULL OUTER JOIN approved_by_policy b
  ON a.policy_id = b.policy_id
ORDER BY policy_id
