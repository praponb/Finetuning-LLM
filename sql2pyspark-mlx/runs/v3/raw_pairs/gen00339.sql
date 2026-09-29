SELECT * FROM (SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, city, sqft FROM stores) PIVOT (MAX(sqft) FOR city IN ('Tokyo', 'Berlin', 'Chiang Mai')) ORDER BY opened_month
