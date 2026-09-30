WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (5, 6, 8, 9)
),
agg AS (
    SELECT YEAR(last_restock) AS last_restock_year, COUNT(DISTINCT quantity) AS unique_stores
    FROM recent_inventory
    GROUP BY YEAR(last_restock)
)
SELECT last_restock_year, unique_stores
FROM agg
