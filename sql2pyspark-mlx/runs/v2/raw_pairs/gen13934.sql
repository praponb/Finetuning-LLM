WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn FROM transactions) SELECT id, txn_type, amount, txn_time FROM Numbered WHERE rn <= 3
