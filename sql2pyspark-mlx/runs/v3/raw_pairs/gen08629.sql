select id, txn_time, account_id, amount, date_format(txn_time, 'MMM yyyy') as txn_time_label from transactions where to_date(txn_time) >= '2024-06-30'
