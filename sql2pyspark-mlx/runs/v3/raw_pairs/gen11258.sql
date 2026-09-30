SELECT MONTH(delivered_date) AS delivered_month, carrier, MAX(cost) AS max_cost FROM shipments GROUP BY MONTH(delivered_date), carrier HAVING MAX(cost) > 65
