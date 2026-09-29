WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rnk FROM payments) SELECT id, status, amount FROM numbered WHERE rnk <= 3;
