select
    a.id,
    a.customer_id,
    t.avg_amount,
    t.merchant_values
from accounts a
inner join (select account_id, avg(amount) as avg_amount, concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values
    from transactions
    group by account_id) t on a.id = t.account_id
order by t.avg_amount, 2 desc
