select
    a.account_type,
    count(t.id) as transaction_count,
    sum(t.amount) as total_amount,
    avg(t.amount) as avg_amount
from accounts a
left join transactions t on a.id = t.account_id
group by a.account_type
order by a.account_type;
