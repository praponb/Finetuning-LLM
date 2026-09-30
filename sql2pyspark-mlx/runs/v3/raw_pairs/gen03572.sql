SELECT store_id, avg_units
FROM (
    SELECT store_id, ROUND(AVG(units), 2) AS avg_units
    FROM store_sales
    WHERE revenue <= 2400
    GROUP BY store_id
) AS agg
WHERE avg_units > 28
ORDER BY store_id DESC, avg_units;
