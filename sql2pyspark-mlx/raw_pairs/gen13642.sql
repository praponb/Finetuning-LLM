SELECT
    id,
    city,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_row
FROM stores
WHERE MONTH(opened_date) IN (2, 7, 10)
