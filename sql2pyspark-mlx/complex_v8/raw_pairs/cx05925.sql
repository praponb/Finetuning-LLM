WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE approved_amount >= 0.0
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id IS NOT NULL
              AND c.claim_date >= DATE '2024-05-14'
        )
),
deduped AS (
    -- Keep the latest row per adjuster_id, claim_type_or_unknown
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        claim_type_or_unknown,
        days_since_reported_at,
        status_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, claim_type_or_unknown ORDER BY approved_amount DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_approved_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM deduped s
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.claim_type <> 'medical' THEN 1 END) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM profiled s
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
        status,
        claim_type_or_unknown,
        days_since_reported_at,
        status_slug,
        min_claim_approved_amount,
        claim_count,
        claim_claim_type_count,
        claim_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_claim_type_count DESC, id DESC) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    claim_type_or_unknown,
    days_since_reported_at,
    status_slug,
    min_claim_approved_amount,
    claim_count,
    claim_claim_type_count,
    claim_count_2,
    ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj,
    DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
FROM latest
WHERE days_since_reported_at < 525
ORDER BY claim_claim_type_count DESC NULLS FIRST, id NULLS LAST
LIMIT 5
