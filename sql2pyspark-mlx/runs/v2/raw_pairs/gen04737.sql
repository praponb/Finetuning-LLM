SELECT MAX(revenue) AS max_revenue, COUNT(DISTINCT revenue) AS unique_stores, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales WHERE revenue < 3600 ORDER BY unique_stores DESC
