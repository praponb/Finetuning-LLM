SELECT * FROM (SELECT order_id, status, amount FROM payments) src PIVOT (SUM(amount) AS total, COUNT(1) AS cnt FOR status IN ('SUCCESS' AS success, 'REFUNDED' AS refunded));
