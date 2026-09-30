WITH Ranked AS (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS pos FROM payments WHERE paid_at >= '2023-08-01') SELECT id, order_id, amount, method FROM Ranked WHERE pos <= 5
