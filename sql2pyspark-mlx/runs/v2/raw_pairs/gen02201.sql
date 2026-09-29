SELECT *
FROM (
    SELECT YEAR(opened_date) AS opened_year, COUNT(DISTINCT sqft) AS unique_citys
    FROM stores
    GROUP BY YEAR(opened_date)
) AS agg
WHERE unique_citys >= 6;
