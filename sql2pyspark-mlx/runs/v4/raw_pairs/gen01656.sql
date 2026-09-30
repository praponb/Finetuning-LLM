WITH top_order_items AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) rn
    FROM order_items
)
SELECT *
FROM top_order_items
WHERE rn <= 2
ORDER BY id;
