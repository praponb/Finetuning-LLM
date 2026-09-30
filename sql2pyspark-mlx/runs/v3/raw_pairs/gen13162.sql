SELECT
    id,
    city,
    opened_date,
    store_name,
    FIRST_VALUE(store_name) OVER (PARTITION BY city ORDER BY opened_date, id) AS first_store_name
FROM stores;
