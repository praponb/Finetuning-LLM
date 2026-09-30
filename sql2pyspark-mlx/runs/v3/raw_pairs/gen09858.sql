SELECT *
FROM (
    SELECT
        id,
        customer_id,
        discount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS pos
    FROM orders
) AS t
WHERE pos = 1;
