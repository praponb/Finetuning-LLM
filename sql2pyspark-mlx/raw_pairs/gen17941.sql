SELECT id, amount FROM orders WHERE order_date <= '2023-03-11' OR shipping_fee BETWEEN 14 AND 15 ORDER BY id DESC, amount DESC;
