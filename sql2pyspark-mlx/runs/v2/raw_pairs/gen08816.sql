SELECT
    id,
    sqft,
    city,
    opened_date,
    CONCAT(store_name, ' - ', region) AS store_name_region,
    WEEKOFYEAR(opened_date) AS opened_date_weekofyear
FROM stores;
