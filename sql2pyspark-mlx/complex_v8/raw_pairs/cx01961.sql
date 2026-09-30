WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE claimed_amount < 43724.9
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date
    FROM cleaned s
),
latest_per_key AS (
    -- Keep the latest row per status_or_unknown
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        status_claim_type_label,
        status_or_unknown,
        last_claim_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status_or_unknown ORDER BY last_claim_date DESC, id DESC) AS recency_rank
        FROM with_counts
    ) ranked
    WHERE recency_rank = 1
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.status_claim_type_label,
        s.last_claim_date,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date,
        c.adjuster_id AS claim_adjuster_id
    FROM latest_per_key s
    JOIN claims c ON c.policy_id = s.policy_id
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.reported_at,
        s.last_claim_date,
        s.claim_id,
        s.claim_approved_amount,
        s.claim_claim_date,
        s.claim_adjuster_id,
        c.id AS claim_id_2,
        c.claim_date AS claim_claim_date_2,
        c.approved_amount AS claim_approved_amount_2
    FROM with_children s
    JOIN claims c ON c.adjuster_id = s.claim_adjuster_id AND c.claim_date BETWEEN DATE '2024-03-30' AND DATE '2025-11-12'
),
deduped AS (
    -- Keep the earliest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at,
        last_claim_date,
        claim_id,
        claim_approved_amount,
        claim_claim_date,
        claim_adjuster_id,
        claim_id_2,
        claim_claim_date_2,
        claim_approved_amount_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claim_claim_date_2, id, claim_id, claim_id_2) AS recency_rank
        FROM exploded_detail_claims
    ) ranked
    WHERE recency_rank = 1
),
latest AS (
    -- Keep the latest row per claim_type, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at,
        last_claim_date,
        claim_id,
        claim_approved_amount,
        claim_claim_date,
        claim_adjuster_id,
        claim_id_2,
        claim_claim_date_2,
        claim_approved_amount_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_claim_date DESC, id DESC, claim_id DESC, claim_id_2 DESC) AS recency_rank
        FROM deduped
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    policy_id,
    claim_type,
    reported_at,
    last_claim_date,
    claim_id,
    claim_approved_amount,
    claim_claim_date,
    claim_claim_date_2,
    claim_approved_amount_2
FROM latest
ORDER BY id
