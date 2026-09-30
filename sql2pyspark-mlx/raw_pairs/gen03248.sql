WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS tier_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        tier_value,
        CUME_DIST() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank,
        RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_rank_2
    FROM lines
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency,
        tier_value_cume_dist,
        tier_value_percent_rank,
        tier_value_rank,
        tier_value_rank_2,
        CASE WHEN tier_value_cume_dist >= 0.68 THEN 'High' WHEN tier_value_cume_dist >= 0.36 THEN 'Medium' ELSE 'Low' END AS tier_value_cume_dist_band
    FROM scored
    WHERE tier_value_rank <= 3
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        tier_value_cume_dist,
        tier_value_percent_rank,
        tier_value_rank,
        tier_value_rank_2,
        CASE WHEN currency IN ('THB', 'EUR') THEN 'Tier 1' ELSE 'Other' END AS currency_group
    FROM cleaned
    WHERE tier_value_rank BETWEEN 3 AND 7
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN tier_value_rank_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_rank_2_tier,
        COUNT(*) AS row_count,
        ROUND(AVG(tier_value_percent_rank), 2) AS avg_tier_value_percent_rank,
        COUNT(*) AS num_rows,
        MIN(tier_value_rank) AS min_tier_value_rank,
        PERCENTILE_APPROX(tier_value_percent_rank, 0.5) AS tier_value_percent_rank_p50
    FROM base
    GROUP BY CASE WHEN tier_value_rank_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING row_count > 1
)
SELECT
    tier_value_rank_2_tier,
    avg_tier_value_percent_rank,
    min_tier_value_rank
FROM summary;
