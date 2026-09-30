SELECT
    id,
    store_name,
    opened_date,
    city
FROM stores
WHERE opened_date <= '2023-07-11' OR store_name IS NOT NULL;
