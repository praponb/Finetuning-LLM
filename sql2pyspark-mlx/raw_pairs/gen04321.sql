select t.merchant, t.account_id, a.status
from transactions t
left outer join accounts a on a.id = t.account_id
where a.opened_date >= '2024-10-01'
order by 3 desc;
