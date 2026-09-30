select quarter(txn_time) as txn_quarter, merchant, count(amount) as amount_count from transactions where account_id in (8, 35, 36) and txn_type != 'DEBIT' group by 1, 2 order by merchant
