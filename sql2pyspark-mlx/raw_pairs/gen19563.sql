SELECT id, txn_type, amount, is_flagged FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS pos FROM transactions) AS t WHERE pos = 1 ORDER BY id ASC;
