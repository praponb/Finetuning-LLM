select id, account_id
from transactions
where amount between 1700 and 2900
union all
select id, account_id
from transactions
where account_id in (3, 5, 17, 22);
