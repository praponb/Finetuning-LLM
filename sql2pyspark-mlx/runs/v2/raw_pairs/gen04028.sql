SELECT
    id,
    opened_date,
    region,
    TRIM(store_name) AS clean_store_name,
    ADD_MONTHS(opened_date, 3) AS opened_date_plus_months
FROM stores
WHERE YEAR(opened_date) >= 2024
