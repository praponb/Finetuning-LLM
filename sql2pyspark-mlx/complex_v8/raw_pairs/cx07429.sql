WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        QUARTER(claim_date) AS claim_date_quarter,
        CASE WHEN claim_type IN ('fire', 'medical') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-02-12 07:00:00'
      AND policy_id IS NOT NULL
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.claim_date_quarter,
        s.claim_type_group,
        s.claim_date_label,
        p.end_date AS policy_end_date,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM base s
    JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Subtotals with GROUPING SETS
SELECT
    status,
    policy_id,
    ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount,
    PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
FROM combined_details
GROUP BY GROUPING SETS ((status, policy_id), (status), (policy_id), ())
ORDER BY status NULLS LAST, policy_id NULLS LAST
