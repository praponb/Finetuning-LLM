WITH Base AS (
    SELECT *, units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE revenue > 1000 AND units <= 29
),
agg AS (
    SELECT
        product_id,
        store_id,
        AVG(units) AS avg_units,
        SUM(units) AS total_units
    FROM Base
    GROUP BY product_id, store_id
)
SELECT *
FROM agg
WHERE total_units > 230
ORDER BY 2
