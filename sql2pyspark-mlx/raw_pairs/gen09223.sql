WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS rnk
    FROM order_items
    WHERE unit_price < 330
)
SELECT *
FROM ranked
WHERE rnk <= 3;
