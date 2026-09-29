SELECT
    product_id,
    revenue,
    units,
    ROUND(units / revenue, 2) AS revenue_per_unit
FROM store_sales
WHERE store_id IN (1, 2, 4) AND units <= 33;
