SELECT s.*, b.account_type
FROM transactions s
LEFT JOIN accounts b
    ON s.account_id = b.id;
