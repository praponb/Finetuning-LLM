select id, txn_time, txn_type, upper(txn_type) txn_type_upper, cast(amount as int) amount_int from transactions where amount < 1600 and txn_time < '2024-08-01';
