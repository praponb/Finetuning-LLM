with cleaned as (
    select
        *,
        last_day(start_date) as start_date_month_end,
        lower(status) as status_lower
    from subscriptions
    where customer_id is not null
      and customer_id in (11, 37, 38)
)
select *
from (
    select
        plan,
        status,
        monthly_fee
    from cleaned
) src
pivot (
    avg(monthly_fee) as amount
    for status in ('ACTIVE' as active, 'CANCELLED' as cancelled, 'PAUSED' as paused)
)
