SELECT
    store_sales.id,
    store_sales.sale_date,
    store_sales.revenue,
    store_sales.units,
    stores.city,
    LOWER(stores.city) AS city_lower
FROM store_sales
INNER JOIN stores ON store_sales.store_id = stores.id;
