select
    a.id,
    a.opened_date,
    t.max_amount,
    t.stddev_amount
from accounts a
inner join (select account_id, max(amount) max_amount, round(stddev(amount), 2) stddev_amount
    from transactions
    where merchant in ('7-Eleven', 'Amazon')
    group by account_id) t on a.id = t.account_id;
