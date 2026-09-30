with AccountSummary as (
    select account_id, round(avg(amount), 2) as avg_amount
    from transactions
    where account_id in (15, 26)
    group by account_id
)
select a.customer_id, A.avg_amount
from accounts a
join AccountSummary A on a.id = A.account_id
