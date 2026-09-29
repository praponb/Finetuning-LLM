SELECT * FROM (SELECT store_id, customer_id, channel, amount FROM orders) src PIVOT (SUM(amount) AS total, COUNT(1) AS item_count FOR channel IN ('web' AS web, 'store' AS store, 'mobile' AS mobile))
