select year(txn_time) as txn_year, max(txn_time) as last_txn_time, max(amount) as max_amount from transactions where is_flagged = false group by year(txn_time) having max_amount > 3900;
