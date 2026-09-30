WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('YEAR', reported_at) AS reported_at_year,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        QUARTER(claim_date) AS claim_date_quarter
    FROM claims
    WHERE (claimed_amount >= 19347.2 OR approved_amount > 0.0)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        CASE WHEN claim_type IN ('fire', 'flood') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM prepared
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_year,
        s.approved_amount_bucket,
        s.claim_date_quarter,
        s.status_ref,
        s.claim_type_group,
        a.team AS adjuster_team,
        a.adjuster_name,
        p.status AS policy_status
    FROM enriched s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
combined_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at_year,
        s.approved_amount_bucket,
        s.claim_date_quarter,
        s.status_ref,
        s.adjuster_team,
        s.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team_2
    FROM with_details_adjuster s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level <= 3
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    reported_at_year,
    approved_amount_bucket,
    claim_date_quarter,
    status_ref,
    adjuster_team,
    adjuster_level,
    INITCAP(TRIM(status)) AS status_clean
FROM combined_details
ORDER BY status_clean DESC, id DESC
