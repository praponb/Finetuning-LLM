select id, txn_type
from transactions
where is_flagged
union all
select id, txn_type
from transactions
where merchant is null
