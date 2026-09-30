SELECT
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    product_id,
    MIN(revenue) AS min_revenue,
    MIN(units) AS min_units
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), product_id;
