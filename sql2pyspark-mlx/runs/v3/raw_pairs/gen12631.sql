WITH RankedPayments AS (SELECT id, order_id, paid_at, method, RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS pos FROM payments) SELECT * FROM RankedPayments WHERE pos <= 5
