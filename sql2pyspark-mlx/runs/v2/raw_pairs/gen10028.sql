WITH RankedOrders AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount, id) AS row_num FROM orders) SELECT id, customer_id, amount, channel FROM RankedOrders WHERE row_num = 1;
