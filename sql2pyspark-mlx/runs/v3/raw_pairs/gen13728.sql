select *
from (
    select customer_id, avg(balance) as avg_balance, count(distinct balance) as unique_account_types
    from accounts
    group by customer_id
) as agg
where avg_balance >= 54000
order by unique_account_types desc, customer_id
limit 5;
