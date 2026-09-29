WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        COUNT(CASE WHEN status IN ('APPROVED', 'OPEN') THEN 1 END) AS claim_status_count,
        COUNT(*) FILTER (WHERE status = 'IN_REVIEW') AS claim_status_count_2
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        COALESCE(c.claim_status_count_2, 0) AS claim_status_count_2
    FROM in_window s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_180d,
    prior_claimed_amount_180d,
    claim_status_count,
    claim_status_count_2
FROM with_counts
ORDER BY claim_date DESC NULLS FIRST
