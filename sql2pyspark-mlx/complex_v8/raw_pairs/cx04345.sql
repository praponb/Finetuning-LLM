WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LOWER(TRIM(claim_type)) AS claim_type_clean,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE policy_id IS NOT NULL
      AND adjuster_id >= 3
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date,
        a.level AS adjuster_level
    FROM filtered s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(policy_status, 'ALL') AS policy_status,
    policy_id,
    COUNT(*) AS row_count,
    SUM(claimed_amount) AS total_claimed_amount
FROM with_parent
GROUP BY GROUPING SETS ((policy_status, policy_id), (policy_status))
ORDER BY policy_status NULLS LAST, policy_id NULLS LAST
