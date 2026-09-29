WITH FilteredOrders AS (
    SELECT *
    FROM orders
    WHERE channel = 'mobile'
),
customer_id_summary AS (
    SELECT customer_id, store_id, SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) completed_count
    FROM FilteredOrders
    GROUP BY customer_id, store_id
)
SELECT customer_id, store_id
FROM customer_id_summary
ORDER BY store_id DESC
