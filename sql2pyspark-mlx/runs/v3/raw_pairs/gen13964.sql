SELECT
    id,
    store_name,
    sqft,
    city,
    LAST_DAY(opened_date) AS opened_date_month_end
FROM stores
WHERE opened_date >= '2023-12-01'
ORDER BY city DESC, id
LIMIT 20;
