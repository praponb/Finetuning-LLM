WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(element, 0) AS element_filled
    FROM flattened
    WHERE element <= 39
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        element,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element_filled NULLS LAST) AS element_filled_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element_filled DESC) AS element_filled_percent_rank,
        PERCENT_RANK() OVER (ORDER BY element NULLS FIRST) AS element_percent_rank,
        MAX(element) OVER (PARTITION BY currency) AS currency_max_element,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element DESC) AS element_percent_rank_2
    FROM base
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        element_filled_dense_rank,
        element_filled_percent_rank,
        currency_max_element,
        element_percent_rank_2,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_percent_rank_3
    FROM enriched
    WHERE element_filled_percent_rank > 0.49
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        element_filled_dense_rank,
        element_filled_percent_rank,
        currency_max_element,
        currency_max_element * element_filled_dense_rank AS currency_max_element_times_element_filled_dense_rank
    FROM metrics
    WHERE currency_max_element >= 61
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN currency_max_element_times_element_filled_dense_rank >= 67 THEN 'HIGH' ELSE 'LOW' END AS currency_max_element_times_element_filled_dense_rank_tier,
    APPROX_PERCENTILE(element_filled_dense_rank, 0.9) AS element_filled_dense_rank_p90,
    COUNT(*) AS num_rows
FROM filtered
GROUP BY CASE WHEN currency_max_element_times_element_filled_dense_rank >= 67 THEN 'HIGH' ELSE 'LOW' END
ORDER BY currency_max_element_times_element_filled_dense_rank_tier;
