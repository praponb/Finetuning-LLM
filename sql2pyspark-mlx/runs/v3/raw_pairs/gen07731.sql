select a.account_type, count(t.id) transaction_count, max(t.amount) max_amount
from accounts a
left join transactions t on a.id = t.account_id
where a.account_type = 'LOAN'
group by a.account_type
