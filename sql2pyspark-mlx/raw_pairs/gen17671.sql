select txn_type, merchant, sum(case when merchant = 'Amazon' then 1 else 0 end) as amazon_count from transactions group by txn_type, merchant having avg(amount) >= 2500;
