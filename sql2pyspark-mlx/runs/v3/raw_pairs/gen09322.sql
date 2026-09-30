SELECT t.id, t.is_flagged, a.status, a.balance FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE t.merchant <> '7-Eleven' ORDER BY balance
