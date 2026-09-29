WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        CAST(age AS STRING) AS age_num,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM customers
    WHERE signup_date >= DATE '2023-04-25'
)
SELECT city
FROM filtered
EXCEPT DISTINCT
SELECT city
FROM customers
WHERE segment NOT IN ('Corporate', 'Consumer');
