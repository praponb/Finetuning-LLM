SELECT t.id, t.txn_type, t.is_flagged, a.account_type, INITCAP(TRIM(a.status)) AS status_title FROM transactions t JOIN accounts a ON t.account_id = a.id;
