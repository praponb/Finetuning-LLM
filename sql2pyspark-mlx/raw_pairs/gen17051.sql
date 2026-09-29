WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        CUME_DIST() OVER w_recent AS tier_value_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist_2
    FROM lines
    WHERE YEAR(effective_from) = 2023
    WINDOW w_recent AS (PARTITION BY YEAR(effective_from) ORDER BY tier_value NULLS LAST)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, tier_value_cume_dist / NULLIF(tier_value_cume_dist_2, 0) AS tier_value_cume_dist_per_tier_value_cume_dist_2
    FROM metrics
    WHERE effective_from > DATE '2024-06-09'
      AND effective_from >= '2024-06-22'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        LAST_DAY(effective_from) AS effective_from_month_end
    FROM filtered
    WHERE effective_from >= DATE '2024-03-14'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist_2 NULLS FIRST) AS tier_value_cume_dist_2_percent_rank,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist NULLS FIRST) AS tier_value_cume_dist_dense_rank
    FROM cleaned
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT
    SELECT id
    FROM price_tiers
    WHERE YEAR(effective_from) >= 2023
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', effective_from_month_end) AS activity_month,
    CASE WHEN tier_value_cume_dist_2_percent_rank >= 0.75 THEN 'HIGH' ELSE 'LOW' END AS tier_value_cume_dist_2_percent_rank_tier,
    ROUND(AVG(tier_value_cume_dist_2_percent_rank), 2) AS avg_tier_value_cume_dist_2_percent_rank,
    AVG(tier_value_cume_dist_2) AS avg_tier_value_cume_dist_2,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', effective_from_month_end) ORDER BY ROUND(AVG(tier_value_cume_dist_2_percent_rank), 2) DESC) AS avg_tier_value_cume_dist_2_percent_rank_dense_rank
FROM reconciled
GROUP BY DATE_TRUNC('MONTH', effective_from_month_end), CASE WHEN tier_value_cume_dist_2_percent_rank >= 0.75 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(AVG(tier_value_cume_dist_2_percent_rank), 2) < 1000000
