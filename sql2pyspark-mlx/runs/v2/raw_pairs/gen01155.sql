SELECT *
FROM (
    SELECT region, COUNT(DISTINCT sqft) AS unique_citys, SUM(CASE WHEN region = 'West' THEN sqft ELSE 0 END) AS west_sqft
    FROM stores
    WHERE sqft = 13850
    GROUP BY region
) AS s
WHERE west_sqft < 38;
