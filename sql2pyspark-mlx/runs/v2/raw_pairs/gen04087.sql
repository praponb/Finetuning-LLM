SELECT MONTH(delivered_date) AS delivered_month, MAX(delivered_date) AS last_delivered_date FROM shipments GROUP BY MONTH(delivered_date) ORDER BY last_delivered_date, delivered_month DESC
