SELECT
    sale_date,
    units,
    product_id,
    revenue * units revenue_per_unit
FROM store_sales
WHERE units BETWEEN 11 AND 15 AND revenue > 1100
ORDER BY product_id;
