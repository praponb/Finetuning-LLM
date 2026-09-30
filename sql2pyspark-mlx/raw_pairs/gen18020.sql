SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, CASE WHEN region IN ('East', 'South') THEN 'tier_1' ELSE 'other' END AS region_group, MAX(sqft) - MIN(sqft) AS sqft_range
    FROM stores
    WHERE opened_date <= '2024-08-09'
    GROUP BY DATE_TRUNC('MONTH', opened_date), CASE WHEN region IN ('East', 'South') THEN 'tier_1' ELSE 'other' END
) AS x
WHERE sqft_range < 76
ORDER BY opened_month, region_group
LIMIT 20;
