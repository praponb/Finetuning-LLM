select
    *,
    sum(amount) over w_recent as running_sum_amount,
    max(amount) over w_recent as running_max_amount,
    avg(amount) over w_recent as running_avg_amount,
    cume_dist() over w_part as amount_cume_dist
from transactions
where amount is null
window w_recent as (partition by account_id order by txn_time, id rows between unbounded preceding and current row), w_part as (partition by month(txn_time), account_id order by amount desc nulls first)
