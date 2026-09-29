SELECT
    id,
    opened_date,
    store_name,
    city,
    LAST_VALUE(store_name) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_name,
    RANK() OVER (PARTITION BY city ORDER BY opened_date) AS rnk
FROM stores
WHERE YEAR(opened_date) >= 2024
