SELECT id, status, measure, value FROM (SELECT id, status, amount, discount FROM orders) src UNPIVOT (value FOR measure IN (amount, discount));
