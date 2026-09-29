select to_date(txn_time) txn_day, max(amount) - min(amount) amount_range from transactions group by to_date(txn_time);
