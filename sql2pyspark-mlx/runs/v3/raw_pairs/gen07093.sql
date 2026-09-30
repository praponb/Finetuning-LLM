SELECT
    id,
    opened_date,
    store_name,
    DATE_ADD(opened_date, 23) AS opened_date_plus_23d
FROM stores
