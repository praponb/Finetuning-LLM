select a.account_type, max(t.amount) - min(t.amount) as amount_range, count(*) as cnt
from transactions t
right join accounts a on a.id = t.account_id
group by a.account_type
order by cnt, account_type;
