WITH Base AS (
    SELECT *
    FROM store_sales
    WHERE product_id = 9 OR sale_date > '2025-06-01'
),
summary AS (
    SELECT
        store_id,
        product_id,
        MAX(sale_date) AS latest_sale_date,
        AVG(units) AS avg_units
    FROM Base
    GROUP BY store_id, product_id
)
SELECT *
FROM summary
WHERE avg_units >= 19
ORDER BY avg_units DESC;
