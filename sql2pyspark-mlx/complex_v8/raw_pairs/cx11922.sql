WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        LOWER(TRIM(status)) AS status_clean,
        LENGTH(TRIM(status)) AS status_len,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE YEAR(claim_date) = 2024
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM base s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
latest_per_key AS (
    -- Keep the latest row per policy_product, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        status_clean,
        status_len,
        approved_amount_plus_claimed_amount,
        policy_premium,
        policy_end_date,
        policy_product,
        adjuster_level,
        adjuster_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_product, policy_id ORDER BY adjuster_level DESC, id DESC) AS row_num
        FROM combined_details
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        ROUND(approved_amount / NULLIF(status_len, 0), 3) AS approved_amount_per_status_len
    FROM latest_per_key
    WHERE approved_amount_plus_claimed_amount < 75000.0
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.status_clean,
        s.status_len,
        s.approved_amount_plus_claimed_amount,
        s.policy_premium,
        s.policy_end_date,
        s.policy_product,
        s.adjuster_level,
        s.adjuster_name,
        s.approved_amount_per_status_len,
        p.premium AS policy_premium_2,
        p.end_date AS policy_end_date_2,
        a.adjuster_name AS adjuster_adjuster_name,
        a.level AS adjuster_level_2
    FROM labeled s
    JOIN policies p ON p.id = s.policy_id AND p.premium < 3532.7
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE p.end_date BETWEEN DATE '2025-04-17' AND DATE '2026-09-19'
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.status_clean,
        s.status_len,
        s.policy_end_date,
        s.policy_product,
        s.adjuster_name,
        s.adjuster_adjuster_name,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM with_parent s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claimed_amount BETWEEN 11235.6 AND 33690.0
)
-- Subtotals with CUBE
SELECT
    claim_id,
    COALESCE(claim_status, 'ALL') AS claim_status,
    status_len,
    ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
    ROUND(AVG(status_len), 2) AS avg_status_len
FROM exploded_detail
GROUP BY CUBE(claim_id, claim_status, status_len)
ORDER BY claim_id NULLS LAST, claim_status NULLS LAST, status_len NULLS LAST
