SELECT *
FROM (
    SELECT region, SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) east_sqft, MAX(opened_date) last_opened_date
    FROM stores
    GROUP BY region
) s
WHERE east_sqft < 37
