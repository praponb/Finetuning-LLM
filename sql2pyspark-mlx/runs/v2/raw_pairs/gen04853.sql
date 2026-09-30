SELECT * FROM (SELECT id, status, amount, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS pos FROM payments) AS ranked WHERE pos <= 3;
