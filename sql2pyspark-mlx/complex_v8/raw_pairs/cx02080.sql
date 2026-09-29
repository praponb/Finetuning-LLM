WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claimed_amount <= 42447.2
      AND reported_at >= TIMESTAMP '2025-01-19 14:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM valid_claims
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM tagged s
),
unique_rows AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_slug,
        claim_count,
        last_claim_date,
        claim_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY last_claim_date, id) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    policy_id,
    claim_type,
    claimed_amount,
    approved_amount,
    reported_at,
    claim_type_slug,
    last_claim_date,
    claim_count_2
FROM unique_rows
