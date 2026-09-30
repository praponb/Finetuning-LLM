SELECT * FROM (SELECT id, amount FROM orders WHERE amount > 500) sub WHERE id < 20;
