SELECT *
FROM (
    SELECT YEAR(last_restock) AS last_restock_year, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    GROUP BY YEAR(last_restock)
) AS t
WHERE avg_quantity >= 52
ORDER BY avg_quantity, last_restock_year;
