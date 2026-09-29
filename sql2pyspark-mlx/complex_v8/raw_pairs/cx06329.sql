WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-02-23' AND DATE '2025-05-31'
),
deduped AS (
    -- Keep the earliest row per claim_type, adjuster_id
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
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount, id) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
with_parent_policy AS (
    -- Join policies
    SELECT s.*, p.start_date AS policy_start_date
    FROM deduped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.policy_start_date,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM with_parent_policy s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(adjuster_team) OVER (PARTITION BY adjuster_id)) AS distinct_adjuster_team_in_adjuster_id,
        ROUND(AVG(approved_amount) OVER (ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_7
    FROM combined_details_adjuster
)
SELECT
    id,
    policy_id,
    claim_date,
    approved_amount,
    status,
    policy_start_date,
    adjuster_name,
    adjuster_level,
    approved_amount_avg_7,
    DAYOFWEEK(policy_start_date) AS policy_start_date_dayofweek
FROM metrics
ORDER BY approved_amount_avg_7
