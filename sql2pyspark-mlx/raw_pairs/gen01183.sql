SELECT *
FROM (
    SELECT
        country,
        city,
        credit_limit
    FROM customers
) src
PIVOT (
    MAX(credit_limit) AS amount,
    COUNT(1) AS cnt
    FOR city IN ('New York' AS new_york, 'Berlin' AS berlin)
)
