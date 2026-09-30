SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    CASE WHEN sqft >= 13279 THEN 'high' WHEN sqft >= 5964 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    MAX(opened_date) AS latest_opened_date,
    ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
WHERE region IN ('East', 'Central')
GROUP BY 1, 2
HAVING ROUND(AVG(sqft), 2) > 25
ORDER BY latest_opened_date ASC, opened_month ASC;
