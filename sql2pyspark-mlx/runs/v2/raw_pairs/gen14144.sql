SELECT city, opened_year, total_count
FROM (
    SELECT city, YEAR(opened_date) AS opened_year, COUNT(*) AS total_count
    FROM stores
    WHERE LOWER(TRIM(store_name)) = 'siam flagship'
    GROUP BY city, YEAR(opened_date)
) AS x
WHERE total_count < 6
ORDER BY opened_year DESC, total_count DESC;
