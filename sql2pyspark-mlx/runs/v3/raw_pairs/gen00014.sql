select id, account_id
from transactions
where amount < 2000
except
select id, account_id
from transactions
where account_id = 15;
