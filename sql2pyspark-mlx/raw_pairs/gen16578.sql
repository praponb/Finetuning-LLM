WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    plan,
    ROUND(AVG(val), 2) AS avg_val,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
FROM exploded
GROUP BY plan
ORDER BY avg_val NULLS LAST
