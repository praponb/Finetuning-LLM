SELECT YEAR(shipped_date) AS shipped_year, YEAR(delivered_date) AS delivered_year, MIN(shipped_date) AS first_shipped_date
FROM shipments
GROUP BY CUBE(YEAR(shipped_date), YEAR(delivered_date))
