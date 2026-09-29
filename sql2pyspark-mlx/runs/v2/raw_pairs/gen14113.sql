SELECT id, city, opened_date
FROM (
    SELECT
        id,
        city,
        opened_date,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date) AS rnk
    FROM stores
) AS ranked
WHERE rnk <= 2
ORDER BY id DESC
