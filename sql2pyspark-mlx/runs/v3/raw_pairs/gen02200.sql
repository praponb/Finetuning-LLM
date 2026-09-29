select id, account_id, cast(amount as int) as amount_int
from transactions
where is_flagged
order by id desc, amount_int desc;
