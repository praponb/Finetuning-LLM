WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn FROM transactions) SELECT id, txn_type, amount, account_id FROM numbered WHERE rn <= 3;
