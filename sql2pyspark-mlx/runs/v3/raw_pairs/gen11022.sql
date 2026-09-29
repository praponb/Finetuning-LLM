SELECT opened_month, min_sqft, max_sqft
FROM (
    SELECT MONTH(opened_date) AS opened_month, MIN(sqft) AS min_sqft, MAX(sqft) AS max_sqft
    FROM stores
    GROUP BY MONTH(opened_date)
) AS agg
WHERE max_sqft < 14100;
