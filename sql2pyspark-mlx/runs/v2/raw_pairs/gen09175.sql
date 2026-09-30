SELECT
    id,
    product_id,
    units,
    sale_date,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) rn,
    revenue - LAG(revenue, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) revenue_change
FROM store_sales
WHERE sale_date BETWEEN '2024-03-01' AND '2025-06-01'
