SELECT
    id,
    revenue,
    store_id,
    product_id
FROM store_sales
WHERE revenue > (SELECT MAX(revenue) FROM store_sales);
