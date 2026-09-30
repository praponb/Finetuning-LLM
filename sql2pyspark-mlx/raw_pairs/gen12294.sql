SELECT store_id, STDDEV_SAMP(units) AS units_stddev, ROUND(AVG(units), 2) AS avg_units, COUNT(DISTINCT id) AS distinct_ids FROM store_sales GROUP BY 1 ORDER BY avg_units
