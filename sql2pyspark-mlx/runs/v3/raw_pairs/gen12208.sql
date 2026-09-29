SELECT *
FROM (
    SELECT product_id, AVG(units) AS avg_units, MAX(units) AS max_units
    FROM store_sales
    GROUP BY product_id
) AS agg
WHERE avg_units >= 18
ORDER BY product_id DESC;
