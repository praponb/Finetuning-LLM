SELECT
    id,
    opened_date,
    store_name,
    LAST_VALUE(store_name) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_name
FROM stores
WHERE sqft = 7134
ORDER BY last_store_name;
