with account_totals as (
    select account_id, round(avg(amount), 2) as avg_amount
    from transactions
    where account_id in (1, 20, 33)
    group by account_id
)
select
    a.id,
    a.opened_date,
    a.status,
    at.avg_amount
from accounts a
inner join account_totals at on a.id = at.account_id
where at.avg_amount >= 64;
