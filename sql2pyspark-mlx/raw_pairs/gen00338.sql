SELECT CASE WHEN region IN ('Central', 'South') THEN 'core' ELSE 'other' END region_group, MIN(sqft) min_sqft, MAX(opened_date) last_opened_date
FROM stores
GROUP BY region_group
HAVING min_sqft > 14200
ORDER BY region_group ASC
LIMIT 5;
