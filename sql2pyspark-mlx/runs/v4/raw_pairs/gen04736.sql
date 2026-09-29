SELECT MAX(revenue) - MIN(revenue) AS revenue_range, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales WHERE product_id IN (2, 4, 7, 10) ORDER BY stddev_revenue DESC
