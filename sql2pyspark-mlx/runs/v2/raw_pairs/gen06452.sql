select * from (select txn_type, count(*) as transaction_count, max(amount) as max_amount from transactions where txn_type = 'FEE' group by txn_type) as sub where max_amount >= 2400;
