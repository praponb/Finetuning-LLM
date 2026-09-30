WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        CASE WHEN claimed_amount >= 32878.5 THEN 'A' WHEN claimed_amount >= 19347.2 THEN 'B' ELSE 'C' END AS claimed_amount_tier
    FROM claims
    WHERE claimed_amount BETWEEN 7204.1 AND 38909.4
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(claimed_amount_filled - AVG(claimed_amount_filled) OVER w_policy_id, 2) AS claimed_amount_filled_vs_policy_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        NTILE(4) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id) AS reported_at_quartile,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id) AS reported_at_dense_rank
    FROM filtered_claims
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.claimed_amount_filled,
        s.claimed_amount_tier,
        s.claimed_amount_filled_vs_policy_id_avg,
        s.reported_at_percent_rank,
        s.reported_at_quartile,
        s.reported_at_dense_rank,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM sequenced s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_details_policy AS (
    -- Join policies
    SELECT s.*, p.end_date AS policy_end_date_2
    FROM with_details s
    JOIN policies p ON p.id = s.policy_id
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.claimed_amount_filled,
        s.claimed_amount_tier,
        s.claimed_amount_filled_vs_policy_id_avg,
        s.reported_at_percent_rank,
        s.reported_at_quartile,
        s.adjuster_name,
        s.adjuster_team,
        s.adjuster_level,
        s.policy_start_date,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM with_details_policy s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium > 2013.8
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount_filled,
        claimed_amount_tier,
        claimed_amount_filled_vs_policy_id_avg,
        reported_at_percent_rank,
        reported_at_quartile,
        adjuster_level,
        policy_product,
        policy_premium,
        LOWER(REGEXP_REPLACE(TRIM(policy_product), ' ', '_')) AS policy_product_slug
    FROM combined_details_policy
)
SELECT *
FROM (
    SELECT claimed_amount_tier, reported_at_quartile, policy_product, policy_premium
    FROM derived
) src
PIVOT (
    MAX(policy_premium)
    FOR policy_product IN ('LIFE', 'TRAVEL')
)
ORDER BY claimed_amount_tier, reported_at_quartile
