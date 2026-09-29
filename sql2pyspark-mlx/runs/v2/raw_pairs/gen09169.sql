select
    a.id,
    a.status,
    count(t.id) as transaction_count,
    sum(t.amount) as total_amount
from accounts a
left join transactions t on a.id = t.account_id
group by a.id, a.status
having count(t.id) > 4
order by id
