SELECT MONTH(opened_date) AS opened_month, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY MONTH(opened_date)
