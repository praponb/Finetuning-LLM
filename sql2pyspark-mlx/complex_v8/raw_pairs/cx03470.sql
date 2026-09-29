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
        LOWER(TRIM(status)) AS status_clean,
        TO_DATE(reported_at) AS reported_at_date,
        QUARTER(reported_at) AS reported_at_quarter
    FROM claims
    WHERE claim_type = 'flood'
),
latest_per_key AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        status_clean,
        reported_at_date,
        reported_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY reported_at_date DESC, id DESC) AS recency_rank
        FROM base_claims
    ) ranked
    WHERE recency_rank = 1
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status_clean,
        s.reported_at_quarter,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.reported_at AS claim_reported_at
    FROM latest_per_key s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND TO_DATE(c.reported_at) < DATE '2025-03-02'
)
SELECT *
FROM (
    SELECT claim_id, claim_reported_at, claim_claim_type, reported_at_quarter
    FROM with_children_claims
) src
PIVOT (
    SUM(reported_at_quarter) AS amount, MAX(reported_at_quarter) AS max_v
    FOR claim_claim_type IN ('delay', 'medical', 'collision', 'fire')
)
ORDER BY claim_id, claim_reported_at
