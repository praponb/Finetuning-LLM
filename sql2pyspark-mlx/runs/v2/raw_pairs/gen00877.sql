SELECT
    MONTH(opened_date) AS opened_month,
    MIN(opened_date) AS first_opened_date,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(sqft) AS sqft_count
FROM stores
GROUP BY 1
ORDER BY opened_month DESC;
