select a.id, a.account_type, count(t.id) as transaction_count
from accounts a
inner join transactions t on t.account_id = a.id
group by a.id, a.account_type;
