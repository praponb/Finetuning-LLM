WITH flattened AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
metrics AS (
    SELECT
        id,
        plan,
        currency,
        element,
        MIN(element) OVER (PARTITION BY plan) AS plan_min_element
    FROM flattened
    WHERE plan = 'basic'
),
metrics_price_tiers AS (
    SELECT
        id,
        plan,
        currency,
        RANK() OVER w_grp AS plan_min_element_rank,
        CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY plan_min_element DESC NULLS LAST) AS plan_min_element_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_min_element DESC) AS plan_min_element_percent_rank
    FROM metrics
    WINDOW w_grp AS (PARTITION BY currency ORDER BY plan_min_element DESC)
),
metrics_price_tiers_2 AS (
    SELECT
        *,
        MIN(plan_min_element_cume_dist) OVER (PARTITION BY currency, plan) AS currency_min_plan_min_element_cume_dist,
        SUM(plan_min_element_percent_rank) OVER (PARTITION BY currency) AS currency_sum_plan_min_element_percent_rank,
        PERCENT_RANK() OVER (ORDER BY plan_min_element_cume_dist NULLS LAST) AS plan_min_element_cume_dist_percent_rank
    FROM metrics_price_tiers
),
base AS (
    SELECT
        id,
        plan,
        currency,
        plan_min_element_rank,
        plan_min_element_cume_dist,
        currency_min_plan_min_element_cume_dist,
        plan_min_element_cume_dist_percent_rank,
        LEAST(currency_min_plan_min_element_cume_dist, plan_min_element_cume_dist) AS least_currency_min_plan_min_element_cume_dist_plan_min_element_cume_dist,
        plan_min_element_percent_rank - plan_min_element_cume_dist AS plan_min_element_percent_rank_minus_plan_min_element_cume_dist
    FROM metrics_price_tiers_2
    WHERE currency_sum_plan_min_element_percent_rank >= 3
)
SELECT
    plan,
    currency,
    plan_min_element_rank,
    plan_min_element_cume_dist,
    plan_min_element_cume_dist_percent_rank,
    least_currency_min_plan_min_element_cume_dist_plan_min_element_cume_dist,
    plan_min_element_percent_rank_minus_plan_min_element_cume_dist
FROM base
ORDER BY plan_min_element_cume_dist_percent_rank
