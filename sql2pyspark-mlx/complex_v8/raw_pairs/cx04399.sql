WITH scoped AS (
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
        reported_at
    FROM claims
    WHERE claim_type IN ('collision', 'fire', 'medical')
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM scoped s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_date < DATE '2025-09-19'
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        claim_id,
        claim_claim_type,
        claim_claim_date,
        claim_reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claim_reported_at DESC, id DESC, claim_id DESC) AS recency_rank
        FROM detail_rows
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(approved_amount * 1.07, 2) AS approved_amount_adj,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        claim_reported_at + INTERVAL 1 DAYS AS claim_reported_at_plus_1d
    FROM latest_per_key
    WHERE approved_amount <= 23120.7
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.claim_id,
        s.claim_claim_type,
        s.claim_reported_at,
        s.approved_amount_bucket,
        c.id AS claim_id_2,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM without_matches s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    claim_id,
    claim_claim_type,
    claim_id_2,
    claim_approved_amount,
    ABS(approved_amount - 14217.6) AS approved_amount_distance,
    UPPER(TRIM(claim_status)) AS claim_status_clean
FROM detail_rows_claims
WHERE adjuster_id IS NOT NULL
