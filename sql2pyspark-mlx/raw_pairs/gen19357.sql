select merchant, txn_type from transactions intersect all select merchant, txn_type from transactions where txn_type = 'CREDIT';
