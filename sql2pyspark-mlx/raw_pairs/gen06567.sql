SELECT
    store_id,
    product_id,
    MAX(units) AS max_units,
    COUNT(DISTINCT units) AS unique_products,
    MIN(sale_date) AS min_sale_date
FROM store_sales
GROUP BY ROLLUP(store_id, product_id);
