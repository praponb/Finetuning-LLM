SELECT
    CASE WHEN sqft >= 12834 THEN 'high' WHEN sqft >= 6689 THEN 'mid' ELSE 'low' END sqft_bucket,
    YEAR(opened_date) opened_year,
    DATE_FORMAT(opened_date, 'yyyy-MM') opened_month,
    SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) west_count,
    MIN(opened_date) first_opened_date
FROM stores
GROUP BY sqft_bucket, opened_year, opened_month
HAVING SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) >= 35
