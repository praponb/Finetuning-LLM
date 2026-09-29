WITH base AS (
    SELECT *, LOWER(country) AS country_lower
    FROM customers
    WHERE signup_date > DATE '2024-01-24'
      AND city IS NOT NULL
      AND age > 46
)
SELECT id, segment
FROM base
INTERSECT ALL
SELECT id, segment
FROM customers
WHERE city = 'Tokyo';
