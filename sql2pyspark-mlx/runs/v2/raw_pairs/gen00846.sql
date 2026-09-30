SELECT
    city,
    opened_year,
    south_count,
    min_opened_date
FROM (
    SELECT
        city,
        YEAR(opened_date) AS opened_year,
        SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS south_count,
        MIN(opened_date) AS min_opened_date
    FROM stores
    WHERE region NOT IN ('West', 'East')
    GROUP BY city, YEAR(opened_date)
) AS sub
WHERE south_count < 31
ORDER BY city;
