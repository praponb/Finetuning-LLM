select
    *,
    round(amount / sum(amount) over (partition by txn_type, account_id), 4) amount_share,
    lag(amount, 1) over (partition by merchant order by amount, id) prev_amount,
    count(amount) over w_running txn_time_to_date_count_amount,
    sum(amount) over (partition by account_id, txn_type order by amount, id rows between unbounded preceding and current row) running_amount
from transactions
window w_running as (partition by to_date(txn_time))
