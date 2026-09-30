SELECT id, status, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rn FROM payments) AS t WHERE rn = 1 ORDER BY status
