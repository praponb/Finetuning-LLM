with account_totals as (
    select account_id, count(amount) as amount_count, avg(amount) as avg_amount
    from transactions
    where account_id = 21
    group by account_id
)
select
    a.id,
    a.opened_date,
    at.amount_count,
    at.avg_amount
from accounts a
inner join account_totals at on a.id = at.account_id
where at.amount_count >= 7
order by amount_count desc
