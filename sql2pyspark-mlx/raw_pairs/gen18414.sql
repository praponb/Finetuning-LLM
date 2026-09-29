WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(segment, 'Unknown') AS segment_filled,
        LENGTH(name) AS name_length
    FROM customers
    WHERE signup_date >= DATE '2023-07-28'
      AND email IS NOT NULL
)
SELECT country, id
FROM base
EXCEPT
SELECT country, id
FROM customers
WHERE city IN ('Tokyo', 'London')
