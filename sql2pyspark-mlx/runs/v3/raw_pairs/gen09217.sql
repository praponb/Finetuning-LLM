SELECT CASE WHEN s.region IN ('West', 'Central') THEN 'core' ELSE 'other' END AS region_group, COUNT(ss.revenue) AS revenue_count, MIN(ss.sale_date) AS min_sale_date
FROM store_sales ss
LEFT OUTER JOIN stores s ON s.id = ss.store_id
WHERE ss.revenue < 3700
GROUP BY CASE WHEN s.region IN ('West', 'Central') THEN 'core' ELSE 'other' END
ORDER BY revenue_count DESC, region_group
