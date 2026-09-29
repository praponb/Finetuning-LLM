SELECT
    id,
    store_name,
    city,
    region,
    INITCAP(TRIM(city)) city_title,
    CONCAT(store_name, ' - ', region) store_name_region
FROM stores
WHERE region = 'Central' AND MONTH(opened_date) IN (2, 3, 9) AND MONTH(opened_date) IN (1, 2, 7);
