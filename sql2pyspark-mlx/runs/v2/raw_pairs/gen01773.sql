SELECT SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores WHERE opened_date >= '2025-08-01' ORDER BY tokyo_count DESC;
