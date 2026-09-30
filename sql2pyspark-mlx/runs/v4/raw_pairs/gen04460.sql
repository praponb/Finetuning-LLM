with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        last_day(start_date) as start_date_month_end
    from subscriptions
    where end_date >= to_date('2023-12-19')
)
-- Running and moving aggregates
select
    *,
    last_value(monthly_fee, true) over (partition by customer_id order by start_date_month_end, id rows between unbounded preceding and current row) as last_known_monthly_fee,
    ntile(10) over (partition by status, customer_id order by monthly_fee desc, id) as monthly_fee_bucket,
    ntile(5) over (partition by status order by monthly_fee, id) as monthly_fee_bucket_2,
    ntile(5) over (partition by status order by monthly_fee desc, id) as monthly_fee_bucket_3,
    avg(monthly_fee) over (partition by status, customer_id order by start_date, id rows between 2 preceding and current row) as monthly_fee_avg_2
from cleaned
