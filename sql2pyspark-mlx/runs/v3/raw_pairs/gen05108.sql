SELECT *
FROM (
    SELECT
        id,
        channel,
        discount,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) AS rnk
    FROM orders
) AS t
WHERE rnk = 1
