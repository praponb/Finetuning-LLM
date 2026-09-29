WITH base AS (
    SELECT
        *,
        CASE WHEN amount >= 1320 THEN 'High' WHEN amount >= 370 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        CEIL(discount) AS discount_whole
    FROM orders
    WHERE customer_id IS NOT NULL
)
SELECT id
FROM base
INTERSECT ALL
SELECT id
FROM orders
WHERE customer_id IN (5, 25, 35);
