with account_summary as (
    select account_id, sum(amount) as total_amount
    from transactions
    group by account_id
)
select a.account_type, a.opened_date, as.total_amount
from accounts a
join account_summary as on a.id = as.account_id
where as.total_amount >= 13000;
