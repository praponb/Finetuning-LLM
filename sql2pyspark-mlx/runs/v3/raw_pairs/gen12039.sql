SELECT
    id,
    revenue,
    sale_date,
    product_id,
    revenue * units revenue_per_unit
FROM store_sales
WHERE product_id IN (1, 2, 6, 7) OR sale_date BETWEEN '2025-01-01' AND '2025-04-18';
