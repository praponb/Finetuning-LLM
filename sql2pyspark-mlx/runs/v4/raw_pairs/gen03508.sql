SELECT *
FROM (
    SELECT
        region,
        city,
        AVG(sqft) AS avg_sqft,
        MAX(opened_date) AS latest_opened_date
    FROM stores
    GROUP BY region, city
) AS sub
WHERE avg_sqft > 11900
ORDER BY 4 DESC, 1 DESC, city
LIMIT 20;
