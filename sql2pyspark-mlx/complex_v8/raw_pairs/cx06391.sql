WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        ABS(claimed_amount - 32878.5) AS claimed_amount_distance,
        CONCAT_WS('|', claim_type, status) AS claim_type_status_label
    FROM joined_ranges
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.claim_type,
        s.status,
        s.product,
        s.premium,
        s.status_slug,
        s.claimed_amount_distance,
        s.claim_type_status_label,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM tagged s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium < 3544.1
),
policy_product_stats AS (
    -- Median claimed_amount_distance per policy_product
    SELECT policy_product, PERCENTILE_APPROX(claimed_amount_distance, 0.5) AS median_claimed_amount_distance
    FROM joined_policy
    GROUP BY policy_product
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount_distance,
        ROUND(s.claimed_amount_distance - g.median_claimed_amount_distance, 2) AS claimed_amount_distance_minus_median
    FROM joined_policy s
    JOIN policy_product_stats g ON g.policy_product = s.policy_product
    WHERE s.claimed_amount_distance >= g.median_claimed_amount_distance
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    claim_type,
    status,
    product,
    claimed_amount_distance,
    claim_type_status_label,
    policy_product,
    policy_premium,
    DAYOFWEEK(claim_date) AS claim_date_dayofweek,
    ROUND(policy_premium * 0.9, 2) AS policy_premium_adj
FROM compared
WHERE status IN ('PAID', 'APPROVED', 'IN_REVIEW')
