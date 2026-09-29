WITH valid AS (
    SELECT
        id,
        email,
        country,
        segment,
        is_active,
        credit_limit,
        age,
        IF(is_active, 'Yes', 'No') AS is_active_label
    FROM customers
    WHERE signup_date >= DATE '2023-05-08'
      AND city IS NOT NULL
)
SELECT *
FROM (
    SELECT
        country,
        segment,
        credit_limit
    FROM valid
) src
PIVOT (
    AVG(credit_limit) AS total
    FOR segment IN ('Corporate' AS corporate, 'Consumer' AS consumer)
)
