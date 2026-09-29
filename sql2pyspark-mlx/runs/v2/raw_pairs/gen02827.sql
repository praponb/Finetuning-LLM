select t.account_id, a.status, a.opened_date
from transactions t
join accounts a on t.account_id = a.id
where a.balance between 19000 and 61000 or a.account_type in ('CREDIT', 'CHECKING');
