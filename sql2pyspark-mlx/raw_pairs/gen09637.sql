SELECT QUARTER(shipped_date) AS shipped_quarter, AVG(cost) AS avg_cost FROM shipments GROUP BY QUARTER(shipped_date) HAVING avg_cost >= 31
