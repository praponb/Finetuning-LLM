SELECT COUNT(revenue) AS revenue_count, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales ORDER BY stddev_revenue DESC, revenue_count;
