SELECT MONTH(delivered_date) AS delivered_month, MIN(shipped_date) AS first_shipped_date FROM shipments GROUP BY MONTH(delivered_date);
