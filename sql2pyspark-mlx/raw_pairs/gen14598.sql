select * from (select txn_type, min(amount) min_amount from transactions where to_date(txn_time) >= '2025-02-01' group by txn_type) agg where min_amount > 2900
