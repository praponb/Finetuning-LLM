WITH base AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (2, 4, 5) AND (store_id IN (2, 3, 5, 6) OR last_restock <= '2023-05-02')
),
Agg AS (
    SELECT
        product_id,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month,
        MIN(last_restock) AS min_last_restock,
        AVG(quantity) AS avg_quantity
    FROM base
    GROUP BY product_id, DATE_FORMAT(last_restock, 'yyyy-MM')
)
SELECT
    product_id,
    last_restock_month,
    min_last_restock,
    avg_quantity
FROM Agg
WHERE avg_quantity >= 170
ORDER BY last_restock_month;
