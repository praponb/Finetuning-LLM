SELECT COUNT(DISTINCT revenue) AS unique_stores, ROUND(STDDEV(units), 2) AS stddev_units, AVG(revenue) AS avg_revenue FROM store_sales ORDER BY 1
