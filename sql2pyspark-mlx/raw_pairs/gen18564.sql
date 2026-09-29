SELECT store_id, SUM(units) AS total_units, COUNT(*) AS cnt, ROUND(SUM(units) * 100.0 / SUM(SUM(units)) OVER (), 2) AS pct_of_total FROM store_sales GROUP BY store_id
