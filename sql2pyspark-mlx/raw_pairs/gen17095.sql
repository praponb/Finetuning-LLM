WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) / 365 AS years_since_effective_from
    FROM exploded
    WHERE element <= 63
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_recent AS years_since_effective_from_cume_dist,
        DENSE_RANK() OVER w_part AS element_dense_rank,
        DENSE_RANK() OVER w_part AS element_dense_rank_2,
        CUME_DIST() OVER w_hist AS element_cume_dist,
        RANK() OVER w_recent AS years_since_effective_from_rank
    FROM cleaned
    WINDOW w_recent AS (ORDER BY years_since_effective_from DESC NULLS LAST), w_part AS (ORDER BY element DESC NULLS FIRST), w_hist AS (PARTITION BY MONTH(effective_from) ORDER BY element DESC)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(years_since_effective_from_cume_dist, element_dense_rank) AS least_years_since_effective_from_cume_dist_element_dense_rank
    FROM enriched
    WHERE effective_from >= DATE '2024-02-28'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        effective_from,
        years_since_effective_from,
        years_since_effective_from_cume_dist,
        element_dense_rank,
        element_dense_rank_2,
        element_cume_dist,
        years_since_effective_from_rank,
        CUME_DIST() OVER (ORDER BY years_since_effective_from DESC) AS years_since_effective_from_cume_dist_2
    FROM filtered
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT *, RANK() OVER (PARTITION BY MONTH(effective_from) ORDER BY years_since_effective_from_cume_dist) AS years_since_effective_from_cume_dist_rank
    FROM scored
)
-- Grouped metrics
SELECT
    CASE WHEN years_since_effective_from_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS years_since_effective_from_rank_tier,
    MAX(years_since_effective_from_cume_dist) AS max_years_since_effective_from_cume_dist,
    APPROX_PERCENTILE(years_since_effective_from_cume_dist_rank, ARRAY(0.25, 0.5, 0.75)) AS years_since_effective_from_cume_dist_rank_quartiles,
    PERCENTILE_APPROX(years_since_effective_from_cume_dist_2, ARRAY(0.25, 0.5, 0.75)) AS years_since_effective_from_cume_dist_2_quartiles,
    APPROX_PERCENTILE(years_since_effective_from_cume_dist, 0.95) AS years_since_effective_from_cume_dist_p95,
    MIN(years_since_effective_from_cume_dist) AS min_years_since_effective_from_cume_dist,
    COUNT(*) AS event_count
FROM enriched_price_tiers
GROUP BY years_since_effective_from_rank_tier;
