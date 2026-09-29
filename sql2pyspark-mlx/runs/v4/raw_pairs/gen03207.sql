WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
),
metrics AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element,
        MAX(element) OVER (PARTITION BY plan, currency) AS plan_max_element,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element) AS element_dense_rank
    FROM flattened
),
enriched AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element,
        plan_max_element,
        element_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY plan_max_element DESC NULLS LAST) AS plan_max_element_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_max_element DESC) AS plan_max_element_cume_dist_2
    FROM metrics
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element_dense_rank DESC) AS element_dense_rank_percent_rank,
        AVG(plan_max_element_cume_dist_2) OVER (PARTITION BY plan, currency) AS plan_avg_plan_max_element_cume_dist_2
    FROM enriched
)
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY element_dense_rank_percent_rank DESC NULLS FIRST) AS element_dense_rank_percent_rank_cume_dist,
    ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency), 4) AS element_dense_rank_share,
    MIN(element) OVER (PARTITION BY currency, plan) AS currency_min_element
FROM scored;
