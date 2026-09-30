SELECT id, status, amount, method FROM (SELECT id, status, amount, method, DENSE_RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS pos FROM payments) AS r WHERE pos <= 5
