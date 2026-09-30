select
    a.id,
    a.account_type,
    t.min_amount,
    t.stddev_amount
from accounts a
inner join (select account_id, min(amount) as min_amount, round(stddev(amount), 2) as stddev_amount
    from transactions
    group by account_id) t on a.id = t.account_id
where t.stddev_amount >= 21
order by a.account_type asc, t.min_amount desc
