SELECT city, SUM(sqft) AS total_sqft, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total FROM stores GROUP BY city ORDER BY total_sqft, city LIMIT 20;
