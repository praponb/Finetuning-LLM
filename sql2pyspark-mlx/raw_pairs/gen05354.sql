SELECT
    id,
    opened_date,
    city,
    store_name,
    LAST_VALUE(store_name) OVER (PARTITION BY MONTH(opened_date), city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_name
FROM stores
WHERE store_name LIKE 'Dow%';
