SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(sqft) AS total_sqft, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total
FROM stores
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
ORDER BY pct_of_total DESC, 1;
