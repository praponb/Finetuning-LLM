WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS element
)
SELECT
    plan,
    CASE WHEN element >= 75 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    SUM(CASE WHEN plan = 'pro' THEN element ELSE 0 END) AS pro_element,
    ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS element_per_basic,
    COUNT(DISTINCT currency) AS distinct_currencies,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count,
    SUM(element) AS total_element,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY SUM(CASE WHEN plan = 'pro' THEN element ELSE 0 END) DESC) AS pro_element_percent_rank
FROM lines
GROUP BY plan, CASE WHEN element >= 75 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY total_element DESC
