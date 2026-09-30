SELECT opened_month, north_count
FROM (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, COUNT(CASE WHEN region = 'North' THEN 1 END) AS north_count
    FROM stores
    GROUP BY DATE_TRUNC('MONTH', opened_date)
) AS agg
WHERE north_count >= 10
ORDER BY north_count DESC, opened_month
LIMIT 50
