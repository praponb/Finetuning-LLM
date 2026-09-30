select t.id, t.amount
from transactions t
where t.account_id in (select id from accounts where opened_date > '2023-07-12')
order by t.amount desc, id
limit 1
