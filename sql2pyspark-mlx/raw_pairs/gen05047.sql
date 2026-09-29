SELECT id, delivered_date, carrier, cost, SUM(cost) OVER (PARTITION BY YEAR(delivered_date), carrier) AS delivered_date_year_sum_cost FROM shipments;
