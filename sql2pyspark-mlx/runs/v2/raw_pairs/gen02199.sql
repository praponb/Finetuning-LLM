SELECT
    store_id,
    revenue,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
    ROUND(revenue * 0.9, 2) AS adjusted_revenue
FROM store_sales
