select * from (select txn_type, max(amount) - min(amount) as amount_range from transactions where account_id = 6 group by txn_type) as s where amount_range >= 25 order by txn_type, amount_range desc;
