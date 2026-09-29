SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY status ORDER BY order_date DESC) AS pos FROM orders) AS t WHERE pos <= 2 ORDER BY amount, store_id DESC;
