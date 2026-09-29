SELECT * FROM (SELECT YEAR(shipped_date) AS shipped_year, status, cost FROM shipments) PIVOT (SUM(cost) FOR status IN ('RETURNED', 'LOST')) ORDER BY shipped_year;
