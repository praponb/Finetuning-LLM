SELECT
    id,
    segment,
    email,
    CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo
FROM customers
WHERE signup_date BETWEEN '2023-09-29' AND '2024-04-25'
ORDER BY is_tokyo DESC
