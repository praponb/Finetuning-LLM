SELECT CASE WHEN country IN ('UK', 'US') THEN 'group_a' ELSE 'other' END AS country_group, SUM(age) AS total_age, RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank
FROM customers
GROUP BY CASE WHEN country IN ('UK', 'US') THEN 'group_a' ELSE 'other' END
ORDER BY total_age DESC, country_group
LIMIT 5;
