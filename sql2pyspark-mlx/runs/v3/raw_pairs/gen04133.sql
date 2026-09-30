SELECT
    store_name,
    sqft,
    region,
    ADD_MONTHS(opened_date, 1) AS opened_date_plus_months,
    UPPER(TRIM(store_name)) AS store_name_normalized
FROM stores;
