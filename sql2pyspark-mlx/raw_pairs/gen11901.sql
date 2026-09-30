select id, merchant, txn_type
from transactions
where account_id in (24, 31)
union
select id, merchant, txn_type
from transactions
where txn_type in ('DEBIT', 'REFUND', 'FEE')
order by merchant;
