with base as (
    select *, date_add(opened_date, 68) as opened_date_plus_68d
    from accounts
    where status in ('FROZEN', 'CLOSED')
)
select
    id,
    customer_id,
    balance,
    opened_date,
    status,
    opened_date_plus_68d,
    row_number() over (partition by customer_id order by opened_date_plus_68d, id) as seq_no,
    avg(balance) over (partition by month(opened_date_plus_68d), account_type order by opened_date_plus_68d, id rows between 5 preceding and 1 preceding) as balance_avg_5_prior
from base
where balance < 72000;
