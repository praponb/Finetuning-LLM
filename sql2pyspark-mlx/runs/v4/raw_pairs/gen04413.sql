SELECT QUARTER(delivered_date) AS delivered_quarter, SUM(cost) AS total_cost FROM shipments WHERE cost >= 55 AND YEAR(shipped_date) >= 2025 GROUP BY delivered_quarter HAVING total_cost >= 180;
