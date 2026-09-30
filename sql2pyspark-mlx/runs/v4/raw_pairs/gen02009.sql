SELECT *
FROM (
    SELECT YEAR(signup_date) AS signup_year, MIN(age) AS min_age, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values
    FROM customers
    WHERE age <> 32
    GROUP BY YEAR(signup_date)
) AS sub
WHERE min_age < 40;
