SELECT
    id,
    sale_date,
    product_id,
    store_id,
    revenue * units AS revenue_per_unit
FROM store_sales
WHERE units < 36 AND store_id = 6;
