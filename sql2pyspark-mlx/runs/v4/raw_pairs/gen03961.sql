with AccountStats as (
    select account_id, round(avg(amount), 2) as avg_amount, count(*) as num_transactions
    from transactions
    where account_id in (16, 24, 40)
    group by account_id
)
select
    a.id,
    a.customer_id,
    A.avg_amount,
    A.num_transactions
from accounts a
left join AccountStats A on a.id = A.account_id
where A.avg_amount > 50
order by num_transactions;
