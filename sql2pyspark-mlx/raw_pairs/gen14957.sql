SELECT id, txn_type, txn_time, CONCAT(merchant, '/', txn_type) merchant_txn_type FROM transactions WHERE txn_type = 'CREDIT' OR merchant != '7-Eleven'
