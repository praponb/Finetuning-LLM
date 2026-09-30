WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        CASE WHEN premium >= 4670.9 THEN 'A' WHEN premium >= 2289.0 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE premium <= 3521.8
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        end_date,
        premium_tier,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY YEAR(end_date), premium_tier)) AS distinct_product_in_end_date_year_premium_tier,
        ROUND(CUME_DIST() OVER (PARTITION BY product, holder_id ORDER BY premium), 3) AS premium_cume_dist,
        COUNT(*) OVER w_end_date_year_premium_tier AS rows_in_end_date_year_premium_tier,
        ROUND(premium - AVG(premium) OVER w_end_date_year_premium_tier, 2) AS premium_vs_end_date_year_premium_tier_avg,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY YEAR(end_date), premium_tier)) AS distinct_holder_id_in_end_date_year_premium_tier
    FROM filtered_policys
    WINDOW w_end_date_year_premium_tier AS (PARTITION BY YEAR(end_date), premium_tier)
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type = 'flood') FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date
    FROM windowed s
)
-- Subtotals with GROUPING SETS
SELECT
    premium_tier,
    distinct_holder_id_in_end_date_year_premium_tier,
    MAX(end_date) AS last_end_date,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    COUNT(DISTINCT premium_tier) AS distinct_premium_tiers
FROM with_counts
GROUP BY GROUPING SETS ((premium_tier, distinct_holder_id_in_end_date_year_premium_tier), (premium_tier), ())
