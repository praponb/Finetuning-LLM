SELECT opened_month, sqft_bucket, total_sqft
FROM (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, CASE WHEN sqft >= 17465 THEN 'high' WHEN sqft >= 2826 THEN 'mid' ELSE 'low' END AS sqft_bucket, SUM(sqft) AS total_sqft
    FROM stores
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM'), CASE WHEN sqft >= 17465 THEN 'high' WHEN sqft >= 2826 THEN 'mid' ELSE 'low' END
) AS agg
WHERE total_sqft > 71000
ORDER BY total_sqft DESC, opened_month, sqft_bucket
LIMIT 1;
