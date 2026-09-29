SELECT *, ROUND(sqft * 100, 2) AS adjusted_sqft
FROM stores
WHERE MONTH(opened_date) IN (8, 10, 12) AND city IN ('London', 'Tokyo', 'Chiang Mai');
