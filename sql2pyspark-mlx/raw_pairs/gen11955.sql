WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
SELECT
    id,
    plan,
    effective_from,
    currency,
    element,
    CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from), plan ORDER BY element DESC NULLS LAST) AS element_cume_dist,
    DENSE_RANK() OVER (ORDER BY element NULLS LAST) AS element_dense_rank,
    COUNT(element) OVER w_ordered AS plan_count_element,
    MIN(element) OVER w_recent AS currency_min_element,
    SUM(element) OVER w_ordered AS plan_sum_element
FROM lines
WHERE effective_from BETWEEN '2023-04-15' AND '2024-10-01'
WINDOW w_ordered AS (PARTITION BY plan), w_recent AS (PARTITION BY currency, plan);
