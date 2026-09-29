WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC, id) AS rn
    FROM orders
    WHERE order_date >= '2024-06-28'
)
SELECT *
FROM ranked
WHERE rn <= 3;
