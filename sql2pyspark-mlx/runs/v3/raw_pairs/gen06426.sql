SELECT MAX(revenue) - MIN(revenue) AS revenue_range, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales WHERE units != 24 AND product_id IN (2, 7, 8, 10) AND units < 28
