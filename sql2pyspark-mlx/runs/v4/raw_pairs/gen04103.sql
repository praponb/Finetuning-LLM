WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS element
),
with_windows AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_percent_rank,
        CUME_DIST() OVER w_recent AS element_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY element) AS element_rank,
        MIN(element) OVER (PARTITION BY plan, currency) AS plan_min_element
    FROM lines
    WINDOW w_recent AS (PARTITION BY currency ORDER BY element NULLS FIRST)
),
metrics AS (
    SELECT
        *,
        MIN(element_rank) OVER (PARTITION BY plan) AS plan_min_element_rank,
        COUNT(element_cume_dist) OVER (PARTITION BY plan, currency) AS plan_count_element_cume_dist,
        RANK() OVER (ORDER BY plan_min_element DESC NULLS FIRST) AS plan_min_element_rank_2
    FROM with_windows
)
SELECT
    id,
    plan,
    currency,
    element,
    element_percent_rank,
    plan_min_element,
    plan_min_element_rank,
    plan_min_element_rank_2,
    CASE WHEN currency RLIKE '^(USD|EUR)$' THEN 'EXTENDED' WHEN currency RLIKE '^(THB)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS currency_zone
FROM metrics
