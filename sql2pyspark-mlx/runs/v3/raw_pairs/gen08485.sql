SELECT merchant, txn_type, INITCAP(TRIM(txn_type)) AS txn_type_title
FROM transactions;
