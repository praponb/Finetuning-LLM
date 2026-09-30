with cleaned as (
    select
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        ceil(monthly_fee) as monthly_fee_whole,
        date_trunc('month', start_date) as start_date_period
    from subscriptions
    where customer_id is not null
      and plan in ('premium', 'standard', 'enterprise')
)
select id, plan
from cleaned
except
select id, plan
from subscriptions
where monthly_fee < 130
