SELECT *
FROM (
    SELECT
        segment,
        country,
        age
    FROM customers
) src
PIVOT (
    SUM(age) AS amount,
    COUNT(1) AS item_count
    FOR country IN ('TH', 'DE')
)
