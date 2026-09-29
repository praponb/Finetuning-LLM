SELECT opened_quarter, new_york_count
FROM (
    SELECT QUARTER(opened_date) AS opened_quarter, COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count
    FROM stores
    GROUP BY QUARTER(opened_date)
) AS s
WHERE new_york_count > 3
ORDER BY opened_quarter DESC, new_york_count DESC;
