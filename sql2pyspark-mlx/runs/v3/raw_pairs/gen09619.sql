SELECT id, status, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS rn FROM payments) AS r WHERE rn = 1 ORDER BY id DESC;
