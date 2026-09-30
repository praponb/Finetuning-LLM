SELECT city, MAX(opened_date) AS latest_opened_date, ROUND((100 * SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_west
FROM stores
WHERE sqft >= 12924
GROUP BY city
HAVING pct_west > 41;
