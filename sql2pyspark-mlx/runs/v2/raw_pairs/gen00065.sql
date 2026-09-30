SELECT YEAR(opened_date) AS opened_year, SUM(sqft) AS total_sqft, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total
FROM stores
WHERE region IN ('East', 'West', 'South')
GROUP BY YEAR(opened_date)
ORDER BY pct_of_total DESC
