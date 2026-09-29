WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) rnk
    FROM stores
    WHERE city = 'Bangkok'
)
SELECT
    id,
    city,
    opened_date,
    store_name
FROM numbered
WHERE rnk <= 2
ORDER BY store_name;
