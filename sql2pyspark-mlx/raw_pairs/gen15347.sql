SELECT
    id,
    sale_date,
    store_id,
    revenue,
    SUM(revenue) OVER (PARTITION BY YEAR(sale_date), store_id) AS sale_date_year_sum_revenue
FROM store_sales;
