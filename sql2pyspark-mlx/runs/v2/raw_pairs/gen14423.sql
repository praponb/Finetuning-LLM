select txn_type, avg(amount) as avg_amount, round(stddev(amount), 2) as stddev_amount, sum(case when txn_type = 'CREDIT' then amount else 0 end) as credit_amount from transactions group by txn_type;
