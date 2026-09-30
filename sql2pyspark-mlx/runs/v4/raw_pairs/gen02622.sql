WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity) rnk
    FROM order_items
    WHERE unit_price BETWEEN 260 AND 360
)
SELECT *
FROM Ranked
WHERE rnk <= 3
