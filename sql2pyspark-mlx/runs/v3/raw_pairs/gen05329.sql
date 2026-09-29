SELECT
    id,
    city,
    age,
    segment
FROM (
    SELECT
        id,
        city,
        age,
        segment,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS rnk
    FROM customers
) AS t
WHERE rnk <= 3
