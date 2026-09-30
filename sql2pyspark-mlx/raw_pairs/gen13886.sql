SELECT shipped_year, avg_cost, unique_statuss
FROM (
    SELECT YEAR(shipped_date) AS shipped_year, ROUND(AVG(cost), 2) AS avg_cost, COUNT(DISTINCT cost) AS unique_statuss
    FROM shipments
    WHERE YEAR(shipped_date) = 2025
    GROUP BY YEAR(shipped_date)
) AS x
WHERE avg_cost >= 37
