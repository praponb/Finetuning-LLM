with account_totals as (
    select account_id, avg(amount) as avg_amount
    from transactions
    group by account_id
)
select a.customer_id, at.avg_amount
from accounts a
left outer join account_totals at on a.id = at.account_id
where at.avg_amount > 3100
