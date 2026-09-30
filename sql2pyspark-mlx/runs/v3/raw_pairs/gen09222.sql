SELECT
    city,
    store_name,
    region,
    LAST_DAY(opened_date) AS opened_date_month_end
FROM stores
WHERE opened_date <= '2024-08-23'
ORDER BY city DESC;
