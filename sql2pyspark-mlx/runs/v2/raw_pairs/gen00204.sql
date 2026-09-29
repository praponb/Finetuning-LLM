SELECT *
FROM (
    SELECT YEAR(opened_date) AS opened_year, QUARTER(opened_date) AS opened_quarter, MAX(sqft) - MIN(sqft) AS sqft_range
    FROM stores
    GROUP BY YEAR(opened_date), QUARTER(opened_date)
) AS x
WHERE sqft_range < 65
