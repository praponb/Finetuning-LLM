select amount, account_id, txn_time, hour(txn_time) as txn_time_hour from transactions where hour(txn_time) between 11 and 18;
