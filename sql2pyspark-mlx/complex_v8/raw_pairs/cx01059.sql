WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        CONCAT_WS('-', claim_type, status) AS claim_type_status_label,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM claims
    WHERE claimed_amount <= 44991.5
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.claim_type_status_label,
        s.claimed_amount_bucket,
        s.claim_date_week,
        p.status AS policy_status,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM scoped_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
unique_rows AS (
    -- Keep the earliest row per adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        claim_type_status_label,
        claimed_amount_bucket,
        claim_date_week,
        policy_status,
        adjuster_name,
        adjuster_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY claim_date, id) AS rn
        FROM combined_details_policy
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(approved_amount - 14993.0) AS approved_amount_distance,
        CASE WHEN adjuster_team IN ('Health', 'Motor') THEN 'group_a' WHEN adjuster_team = 'Property' THEN 'group_b' ELSE 'other' END AS adjuster_team_group
    FROM unique_rows
)
SELECT
    id,
    adjuster_id,
    claim_type,
    approved_amount,
    status,
    claim_type_status_label,
    claimed_amount_bucket,
    claim_date_week,
    policy_status,
    adjuster_name,
    adjuster_team,
    approved_amount_distance,
    adjuster_team_group
FROM labeled
WHERE claim_type_status_label IS NOT NULL
ORDER BY claim_date_week DESC, adjuster_name, id
LIMIT 5
