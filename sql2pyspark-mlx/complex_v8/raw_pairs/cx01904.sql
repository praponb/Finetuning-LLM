WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        UPPER(TRIM(status)) AS status_clean,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE status NOT IN ('PAID', 'IN_REVIEW')
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(reported_at) AS first_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM filtered_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    approved_amount,
    status,
    status_clean,
    first_claim_reported_at,
    claim_count,
    DATE_FORMAT(first_claim_reported_at, 'yyyy-MM-dd') AS first_claim_reported_at_label,
    DATE_FORMAT(first_claim_reported_at, 'yyyy-MM-dd') AS first_claim_reported_at_label_2
FROM without_matches
ORDER BY first_claim_reported_at_label_2 DESC
