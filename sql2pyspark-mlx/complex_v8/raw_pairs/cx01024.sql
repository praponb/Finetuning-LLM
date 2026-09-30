WITH recent AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claim_type IN ('fire', 'flood')
),
tagged AS (
    -- Add calculated columns
    SELECT *, UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM recent
),
latest_per_key AS (
    -- Keep the earliest row per adjuster_id
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
        claim_type_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claim_date, id) AS rn
        FROM tagged
    ) ranked
    WHERE rn = 1
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type
    FROM latest_per_key s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND TO_DATE(c.reported_at) < DATE '2025-02-18'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        SUM(approved_amount) AS total_claim_approved_amount,
        MIN(claim_date) AS first_claim_date,
        COUNT(CASE WHEN status IN ('REJECTED', 'APPROVED', 'OPEN') THEN 1 END) AS claim_status_count
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount,
        c.first_claim_date,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM line_level_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
deduped AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        claim_id,
        claim_reported_at,
        claim_claimed_amount,
        claim_claim_date,
        claim_claim_type,
        total_claim_approved_amount,
        first_claim_date,
        claim_status_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY first_claim_date DESC, id DESC, claim_id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
)
SELECT
    claim_status_count,
    claim_id,
    COALESCE(claim_claim_type, 'ALL') AS claim_claim_type,
    MIN(claimed_amount) AS min_claimed_amount,
    COUNT(*) AS n_rows,
    DATEDIFF(MAX(first_claim_date), MIN(first_claim_date)) AS first_claim_date_span_days
FROM deduped
GROUP BY CUBE(claim_status_count, claim_id, claim_claim_type)
ORDER BY claim_status_count NULLS LAST, claim_id NULLS LAST, claim_claim_type NULLS LAST
