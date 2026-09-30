SELECT
    QUARTER(sale_date) AS sale_quarter,
    ROUND(AVG(units), 2) AS avg_units,
    MAX(units) - MIN(units) AS units_range,
    ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
GROUP BY QUARTER(sale_date)
ORDER BY avg_units DESC
