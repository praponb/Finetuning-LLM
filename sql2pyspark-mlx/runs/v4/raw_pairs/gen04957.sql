select
    a.id,
    a.opened_date,
    a.customer_id,
    t.total_amount
from accounts a
join (select account_id, sum(amount) as total_amount
    from transactions
    group by account_id) t on a.id = t.account_id
where t.total_amount >= 9000;
