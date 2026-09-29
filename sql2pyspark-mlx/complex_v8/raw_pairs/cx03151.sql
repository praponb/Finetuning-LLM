WITH base_claims AS (
    -- Filter claims
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
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM base_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type IN ('delay', 'fire', 'theft')
        )
),
unique_rows AS (
    -- Keep the latest row per status, policy_id
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
        approved_amount_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, policy_id ORDER BY approved_amount_bucket DESC, id DESC) AS row_num
        FROM with_activity
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    approved_amount_bucket,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM unique_rows
ORDER BY approved_amount_bucket
