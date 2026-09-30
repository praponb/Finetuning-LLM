select txn_type, account_id
from transactions
where account_id = 35
union
select txn_type, account_id
from transactions
where amount is not null
order by txn_type;
