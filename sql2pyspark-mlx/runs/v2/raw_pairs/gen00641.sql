SELECT *
FROM (
    SELECT QUARTER(shipped_date) AS shipped_quarter, ROUND(STDDEV(cost), 2) AS stddev_cost, MIN(cost) AS min_cost
    FROM shipments
    WHERE delivered_date BETWEEN '2023-04-13' AND '2023-12-28'
    GROUP BY QUARTER(shipped_date)
) AS agg
WHERE min_cost >= 56;
