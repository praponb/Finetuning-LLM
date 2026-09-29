SELECT
    id,
    store_name,
    city,
    YEAR(opened_date) AS opened_date_year
FROM stores
ORDER BY opened_date_year DESC, id
LIMIT 50
