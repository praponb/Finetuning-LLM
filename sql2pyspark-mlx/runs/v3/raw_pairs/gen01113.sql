SELECT CASE WHEN category IN ('Home', 'Sports') THEN 'tier_1' ELSE 'other' END category_group, MONTH(launch_date) launch_month, SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) acme_cost
FROM products
WHERE NOT (brand IS NOT NULL AND brand = 'Acme')
GROUP BY category_group, launch_month
HAVING SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) > 56
ORDER BY category_group DESC;
