select a.balance, a.customer_id, t.is_flagged
from accounts a
inner join transactions t on t.account_id = a.id
order by customer_id desc
