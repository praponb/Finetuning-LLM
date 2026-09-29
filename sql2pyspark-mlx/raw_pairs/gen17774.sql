SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM suppliers
) src
PIVOT (
    AVG(rating) AS value_sum,
    COUNT(1) AS n
    FOR city IN ('Bangkok' AS bangkok, 'London' AS london, 'Berlin' AS berlin)
)
