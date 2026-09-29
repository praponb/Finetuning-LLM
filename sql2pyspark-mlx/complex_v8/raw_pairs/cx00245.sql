WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM claims
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        days_since_claim_date,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj
    FROM cleaned_claims
    WHERE claim_type NOT IN ('theft', 'flood')
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.days_since_claim_date,
        s.claim_type_len,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM tagged s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.level >= 2
),
enriched AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.days_since_claim_date,
        s.claim_type_len,
        s.adjuster_level,
        s.adjuster_name,
        a.level AS adjuster_level_2,
        p.product AS policy_product
    FROM joined s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
        )
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    days_since_claim_date,
    adjuster_level,
    adjuster_level_2,
    policy_product,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
    claim_type_len - adjuster_level_2 AS claim_type_len_minus_adjuster_level_2
FROM qualified
ORDER BY claim_type_slug NULLS LAST
