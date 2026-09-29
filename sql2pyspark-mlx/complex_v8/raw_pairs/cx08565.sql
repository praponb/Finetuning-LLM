WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id <= 23
),
combined_details AS (
    -- Attach policy attributes
    SELECT s.*, p.status AS policy_status, p.end_date AS policy_end_date
    FROM base_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
combined_details_2 AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM combined_details s
    JOIN adjusters a ON a.id = s.adjuster_id
),
scored AS (
    -- Label and score rows
    SELECT *, DATE_ADD(policy_end_date, 30) AS policy_end_date_plus_30d
    FROM combined_details_2
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.policy_status,
        s.policy_end_date,
        s.adjuster_name,
        s.adjuster_level,
        s.adjuster_team,
        s.policy_end_date_plus_30d,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        a.team AS adjuster_team_2
    FROM scored s
    JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
),
latest AS (
    -- Keep the latest row per policy_id, adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        policy_status,
        policy_end_date,
        adjuster_name,
        adjuster_level,
        adjuster_team,
        policy_end_date_plus_30d,
        policy_product,
        policy_start_date,
        adjuster_team_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_team ORDER BY policy_end_date_plus_30d DESC, id DESC) AS recency_rank
        FROM with_details_policy
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    policy_end_date,
    adjuster_name,
    adjuster_level,
    adjuster_team,
    policy_end_date_plus_30d,
    policy_product,
    policy_start_date,
    adjuster_team_2,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
    COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
FROM latest
ORDER BY policy_start_date
