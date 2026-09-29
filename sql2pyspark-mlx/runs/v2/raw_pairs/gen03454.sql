SELECT
    units,
    product_id,
    store_id,
    revenue * units AS revenue_per_unit,
    ROUND(units / revenue, 2) AS revenue_per_unit_2
FROM store_sales
WHERE product_id IN (1, 5, 10) OR YEAR(sale_date) >= 2024;
