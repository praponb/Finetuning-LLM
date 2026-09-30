WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM claims
    WHERE policy_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        claimed_amount_bucket,
        status_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY reported_at DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status_ref), ' ', '_')) AS status_ref_slug,
        LENGTH(TRIM(status_ref)) AS status_ref_len,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM latest
    WHERE TO_DATE(reported_at) < DATE '2025-02-18'
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    reported_at,
    claimed_amount_bucket,
    status_ref_slug,
    days_since_claim_date
FROM annotated
ORDER BY id DESC NULLS FIRST, adjuster_id DESC
