SELECT t.id, t.is_flagged, a.balance, a.customer_id, a.account_type, IF(t.is_flagged, 'Yes', 'No') AS is_flagged_label FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id;
