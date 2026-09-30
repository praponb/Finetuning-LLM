WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status
    FROM claims
    WHERE adjuster_id >= 3
),
unique_rows AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id DESC) AS recency_rank
        FROM cleaned_claims
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Add calculated columns
    SELECT *, COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM unique_rows
    WHERE claim_type NOT IN ('theft', 'medical')
)
SELECT
    policy_id,
    status,
    claim_type_or_unknown,
    GROUPING(policy_id) AS is_total_policy_id,
    COUNT(*) FILTER (WHERE claim_type = 'fire') AS n_claim_type_fire,
    COUNT(*) AS n_rows
FROM derived
GROUP BY ROLLUP(policy_id, status, claim_type_or_unknown)
