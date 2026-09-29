SELECT
    id,
    segment,
    age,
    signup_date
FROM (
    SELECT
        id,
        segment,
        age,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS pos
    FROM customers
    WHERE country = 'DE'
) AS ranked
WHERE pos = 1;
