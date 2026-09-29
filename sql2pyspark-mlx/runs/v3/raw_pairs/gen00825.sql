SELECT
    opened_year,
    city,
    min_sqft,
    unique_regions
FROM (
    SELECT
        YEAR(opened_date) AS opened_year,
        city,
        MIN(sqft) AS min_sqft,
        COUNT(DISTINCT sqft) AS unique_regions
    FROM stores
    WHERE city IN ('Bangkok', 'Chiang Mai', 'New York')
    GROUP BY YEAR(opened_date), city
) AS agg
WHERE min_sqft >= 11900;
