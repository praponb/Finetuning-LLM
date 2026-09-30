select a.balance, a.account_type, t.amount
from accounts a
left outer join transactions t on a.id = t.account_id
where t.account_id in (2, 7, 21, 40)
order by balance asc
