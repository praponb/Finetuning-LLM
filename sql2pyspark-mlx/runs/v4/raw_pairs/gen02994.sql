SELECT city, sg_count
FROM (
    SELECT city, SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END) AS sg_count
    FROM suppliers
    WHERE supplier_name NOT LIKE 'To%'
    GROUP BY city
) AS x
WHERE sg_count > 49
ORDER BY sg_count DESC
