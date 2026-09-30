WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len,
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE adjuster_id <= 6
),
with_details_policy AS (
    -- Join policies
    SELECT s.*, p.start_date AS policy_start_date
    FROM filtered s
    JOIN policies p ON p.id = s.policy_id
),
latest AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        status_len,
        claim_type_clean,
        policy_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id DESC) AS row_num
        FROM with_details_policy
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.status_len,
        s.claim_type_clean,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM latest s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT *
FROM (
    SELECT adjuster_id, claim_date, claim_type, approved_amount
    FROM enriched
) src
PIVOT (
    COUNT(approved_amount)
    FOR claim_type IN ('theft', 'medical', 'flood', 'collision')
)
ORDER BY adjuster_id, claim_date
