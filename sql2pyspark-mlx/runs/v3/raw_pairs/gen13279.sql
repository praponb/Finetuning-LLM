select merchant, txn_type from transactions where txn_time >= '2023-08-05' union select merchant, txn_type from transactions where txn_type in ('CREDIT', 'REFUND');
