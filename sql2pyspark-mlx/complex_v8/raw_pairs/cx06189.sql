WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
latest_per_key AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY premium DESC, id DESC) AS row_num
        FROM matched
    ) ranked
    WHERE row_num = 1
),
per_group AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        claim_type,
        ROUND(SUM(claimed_amount) / NULLIF(SUM(premium), 0), 3) AS claimed_amount_to_premium_ratio,
        SUM(CASE WHEN product = 'AUTO' THEN 1 ELSE 0 END) AS n_product_auto,
        ROUND(100 * COUNT(CASE WHEN claim_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_claim_type_is_not_null,
        SUM(CASE WHEN claim_type = 'fire' THEN claimed_amount ELSE 0 END) AS claimed_amount_claim_type_fire,
        COUNT(DISTINCT product) AS distinct_products
    FROM latest_per_key
    WHERE claim_type <> 'theft'
    GROUP BY adjuster_id, claim_type
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount_to_premium_ratio,
        s.n_product_auto,
        s.pct_claim_type_is_not_null,
        s.claimed_amount_claim_type_fire,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM per_group s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.team = 'Travel'
)
-- Final projection
SELECT
    adjuster_id,
    claim_type,
    n_product_auto,
    pct_claim_type_is_not_null,
    claimed_amount_claim_type_fire,
    LOWER(REGEXP_REPLACE(TRIM(adjuster_team), ' ', '_')) AS adjuster_team_slug
FROM joined_adjuster
ORDER BY adjuster_team_slug DESC, adjuster_id, claim_type
LIMIT 20
