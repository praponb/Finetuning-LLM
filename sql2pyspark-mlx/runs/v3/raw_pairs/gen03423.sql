select a.account_type, t.merchant, t.account_id
from accounts a
inner join transactions t on a.id = t.account_id
order by account_id desc;
