WITH base AS (
    SELECT *
    FROM store_sales
    WHERE (revenue < 3600 AND revenue >= 3700) OR product_id IN (2, 3, 7, 9)
),
Agg AS (
    SELECT store_id, MAX(sale_date) AS last_sale_date
    FROM base
    GROUP BY store_id
)
SELECT *
FROM Agg
