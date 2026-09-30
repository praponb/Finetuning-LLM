with scored as (
    select
        *,
        max(balance) over w_part as running_max_balance,
        sum(balance) over w_part as running_sum_balance,
        avg(balance) over w_part as running_avg_balance,
        lag(balance, 1) over (partition by year(opened_date), account_type order by balance asc, id asc) as prev_balance
    from accounts
    window w_part as (partition by status order by balance asc, id asc rows between unbounded preceding and current row)
)
select
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    running_max_balance,
    running_sum_balance,
    prev_balance
from scored
