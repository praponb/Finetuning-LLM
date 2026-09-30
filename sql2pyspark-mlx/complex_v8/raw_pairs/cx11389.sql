WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 23207.7 THEN 'A' WHEN approved_amount >= 4868.7 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM claims
    WHERE status = 'APPROVED'
      AND claim_date >= DATE '2024-06-26'
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.reported_at,
        s.approved_amount_tier,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM cleaned_claims s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
unique_rows AS (
    -- Keep the latest row per policy_status
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        approved_amount_tier,
        policy_status,
        policy_holder_id,
        policy_product,
        adjuster_level,
        adjuster_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_status ORDER BY adjuster_level DESC, id DESC) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount_tier,
        policy_status,
        policy_holder_id,
        adjuster_level,
        adjuster_name,
        TO_DATE(reported_at) AS reported_at_date
    FROM unique_rows
    WHERE approved_amount_tier IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_level,
    policy_id,
    MIN(adjuster_level) AS min_adjuster_level,
    SUM(CASE WHEN policy_status = 'ACTIVE' THEN 1 ELSE 0 END) AS n_policy_status_active,
    ROUND(AVG(adjuster_level), 2) AS avg_adjuster_level
FROM derived
GROUP BY GROUPING SETS ((adjuster_level, policy_id), (adjuster_level), (policy_id), ())
