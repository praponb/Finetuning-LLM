WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
valid AS (
    SELECT *, INITCAP(TRIM(currency)) AS currency_title
    FROM flattened
    WHERE currency NOT IN ('THB', 'USD')
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(effective_from), currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        DENSE_RANK() OVER w_ordered AS tier_value_dense_rank_2
    FROM valid
    WINDOW w_ordered AS (ORDER BY tier_value DESC)
),
filtered AS (
    SELECT
        id,
        effective_from,
        currency,
        currency_title,
        tier_value_cume_dist,
        tier_value_dense_rank,
        tier_value_dense_rank_2,
        COALESCE(tier_value_dense_rank_2, 0) AS tier_value_dense_rank_2_filled
    FROM metrics
    WHERE effective_from > TO_DATE('2024-04-11')
),
filtered_price_tiers AS (
    SELECT
        id,
        effective_from,
        currency,
        currency_title,
        tier_value_cume_dist,
        tier_value_dense_rank,
        tier_value_dense_rank_2,
        tier_value_dense_rank_2_filled,
        tier_value_cume_dist + tier_value_dense_rank_2_filled AS tier_value_cume_dist_plus_tier_value_dense_rank_2_filled
    FROM filtered
    WHERE currency = 'THB'
),
remaining_keys AS (
    SELECT id, currency
    FROM filtered_price_tiers
    EXCEPT
    SELECT id, currency
    FROM price_tiers
    WHERE plan = 'basic'
),
kept_rows AS (
    SELECT s.*
    FROM filtered_price_tiers s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.currency = k.currency
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    APPROX_PERCENTILE(tier_value_dense_rank_2, ARRAY(0.25, 0.5, 0.75)) AS tier_value_dense_rank_2_quartiles,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    ROUND(SUM(tier_value_cume_dist) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS tier_value_cume_dist_per_thb
FROM kept_rows
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', effective_from), DATE_TRUNC('QUARTER', effective_from)), (DATE_TRUNC('WEEK', effective_from)), ())
