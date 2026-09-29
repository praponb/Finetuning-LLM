select
    id,
    is_flagged,
    merchant,
    amount,
    datediff(current_date(), txn_time) as days_since_txn_time
from transactions
where txn_type not in ('CREDIT', 'FEE', 'REFUND') and account_id in (13, 15) and account_id = 20
order by merchant;
