SELECT
    id,
    region,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS opened_date_row
FROM stores
WHERE store_name IS NOT NULL
