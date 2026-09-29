WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS rnk
    FROM orders
)
SELECT id, customer_id, discount
FROM Ranked
WHERE rnk = 1
ORDER BY id DESC, discount DESC;
