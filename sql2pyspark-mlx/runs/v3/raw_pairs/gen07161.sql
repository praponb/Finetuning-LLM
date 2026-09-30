select a.id, count(t.id) as transaction_count
from accounts a
left outer join transactions t on a.id = t.account_id
group by a.id
having count(t.id) >= 3
