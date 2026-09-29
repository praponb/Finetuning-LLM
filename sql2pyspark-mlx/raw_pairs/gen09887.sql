SELECT city, DATE_FORMAT(opened_date, 'yyyy-MM') opened_date_label
FROM stores
WHERE city IN ('Chiang Mai', 'Bangkok')
