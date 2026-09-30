SELECT *, DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time FROM transactions WHERE merchant = 'Lazada' ORDER BY 8 DESC, txn_time ASC;
