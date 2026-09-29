SELECT *
FROM (
    SELECT customer_id, MAX(amount) - MIN(amount) amount_range, SUM(CASE WHEN status = 'COMPLETED' THEN discount ELSE 0 END) completed_discount
    FROM orders
    GROUP BY customer_id
) x
WHERE amount_range > 72
