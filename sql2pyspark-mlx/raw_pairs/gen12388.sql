SELECT location, CASE WHEN location IN ('Tokyo', 'New York') THEN 'tier_1' ELSE 'other' END location_group, SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) finance_count
FROM departments
WHERE dept_name = 'Finance' AND dept_name IN ('Legal', 'HR')
GROUP BY location, CASE WHEN location IN ('Tokyo', 'New York') THEN 'tier_1' ELSE 'other' END;
