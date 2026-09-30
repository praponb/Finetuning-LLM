WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(currency) AS currency_upper,
        ROUND(element * 1.1, 2) AS element_scaled
    FROM lines
    WHERE element >= 79
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        element,
        currency_upper,
        CUME_DIST() OVER w_grp AS element_scaled_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS LAST) AS element_percent_rank,
        AVG(element) OVER (PARTITION BY plan, currency) AS plan_avg_element,
        MIN(element_scaled) OVER (PARTITION BY currency, plan) AS currency_min_element_scaled
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY plan ORDER BY element_scaled DESC NULLS FIRST)
),
enriched_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_recent AS element_scaled_cume_dist_cume_dist,
        MAX(element_scaled_cume_dist) OVER w_recent_2 AS currency_max_element_scaled_cume_dist
    FROM enriched
    WINDOW w_recent AS (PARTITION BY currency ORDER BY element_scaled_cume_dist DESC NULLS LAST), w_recent_2 AS (PARTITION BY currency)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        currency,
        element,
        currency_upper,
        element_scaled_cume_dist,
        currency_min_element_scaled,
        PERCENT_RANK() OVER (ORDER BY element_percent_rank DESC) AS element_percent_rank_percent_rank,
        AVG(element_percent_rank) OVER (PARTITION BY currency) AS currency_avg_element_percent_rank
    FROM enriched_price_tiers
)
-- Aggregate with conditional metrics
SELECT
    currency,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    ROUND(AVG(element_scaled_cume_dist), 2) AS avg_element_scaled_cume_dist
FROM scored
GROUP BY currency
