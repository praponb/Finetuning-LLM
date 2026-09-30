WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS element
)
SELECT
    *,
    COUNT(element) OVER (PARTITION BY currency, plan) AS currency_count_element,
    DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY element DESC) AS element_dense_rank
FROM exploded
