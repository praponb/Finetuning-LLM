SELECT id, status, amount, method FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) pos FROM payments) t WHERE pos <= 3 ORDER BY status ASC, 3 DESC;
