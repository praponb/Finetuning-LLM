SELECT id, is_flagged, SUBSTRING(txn_type, 1, 2) AS txn_type_prefix, INITCAP(TRIM(merchant)) AS merchant_title FROM transactions WHERE amount BETWEEN 2900 AND 3100;
