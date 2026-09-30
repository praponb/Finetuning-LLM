WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE status <> 'OPEN'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM valid_claims
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claim_type_len,
        p.start_date AS policy_start_date,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM tagged s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
deduped AS (
    -- Keep the earliest row per adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_len,
        policy_start_date,
        adjuster_name,
        adjuster_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY claim_type_len, id) AS row_num
        FROM combined_details_policy
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    claim_type_len,
    adjuster_id,
    policy_id,
    ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount
FROM deduped
GROUP BY CUBE(claim_type_len, adjuster_id, policy_id)
ORDER BY claim_type_len NULLS LAST, adjuster_id NULLS LAST, policy_id NULLS LAST
