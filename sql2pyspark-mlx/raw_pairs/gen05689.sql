SELECT
    CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    MAX(opened_date) AS latest_opened_date,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
GROUP BY 1, 2;
