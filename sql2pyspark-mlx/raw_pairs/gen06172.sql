WITH numbered AS (
    SELECT
        id,
        customer_id,
        discount,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS rnk
    FROM orders
)
SELECT
    id,
    customer_id,
    discount,
    status
FROM numbered
WHERE rnk = 1;
