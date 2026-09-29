WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-04-27' AND DATE '2025-04-25'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date,
        p.product AS policy_product
    FROM prepared_claims s
    JOIN policies p ON p.id = s.policy_id
),
per_group AS (
    -- Group-level totals
    SELECT
        policy_product,
        claim_type_len,
        ROUND(100 * COUNT(CASE WHEN policy_product IN ('TRAVEL', 'HOME', 'HEALTH') THEN 1 END) / COUNT(*), 1) AS pct_policy_product_in_travel_hom,
        SUM(CASE WHEN policy_product IN ('HOME', 'HEALTH', 'AUTO') THEN claim_type_len ELSE 0 END) AS claim_type_len_policy_product_in_home_healt,
        MAX(claimed_amount) AS max_claimed_amount,
        ROUND(SUM(claimed_amount) / NULLIF(SUM(claim_type_len), 0), 3) AS claimed_amount_to_claim_type_len_ratio,
        ROUND(SUM(claim_type_len) / NULLIF(SUM(approved_amount), 0), 3) AS claim_type_len_to_approved_amount_ratio
    FROM joined
    WHERE policy_end_date BETWEEN DATE '2025-12-05' AND DATE '2027-01-03'
    GROUP BY 1, 2
    HAVING MAX(claimed_amount) > 15381.6
)
-- Final projection
SELECT
    policy_product,
    claim_type_len,
    pct_policy_product_in_travel_hom,
    claim_type_len_policy_product_in_home_healt,
    max_claimed_amount,
    claimed_amount_to_claim_type_len_ratio,
    claim_type_len_to_approved_amount_ratio,
    LENGTH(TRIM(policy_product)) AS policy_product_len,
    LOWER(REGEXP_REPLACE(TRIM(policy_product), ' ', '_')) AS policy_product_slug
FROM per_group
WHERE claim_type_len <= 12
ORDER BY claim_type_len
