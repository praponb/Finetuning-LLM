SELECT YEAR(shipped_date) AS shipped_year, MIN(cost) AS min_cost FROM shipments GROUP BY shipped_year;
