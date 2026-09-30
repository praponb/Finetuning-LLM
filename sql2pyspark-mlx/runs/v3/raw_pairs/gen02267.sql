SELECT
    store_id,
    SUM(units) AS total_units,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(units) DESC) AS units_rank
FROM store_sales
GROUP BY store_id
