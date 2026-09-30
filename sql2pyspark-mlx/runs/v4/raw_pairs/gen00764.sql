with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, length(status) as status_length
    from subscriptions
    where customer_id is not null
),
base as (
    -- Basic cleansing and defaults
    select *, cast(monthly_fee as int) as monthly_fee_int
    from cleaned
    where end_date > date '2023-08-24'
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by status order by start_date nulls last, id) = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_length,
        monthly_fee_int,
        last_value(monthly_fee) ignore nulls over (partition by plan order by end_date, id rows between unbounded preceding and current row) as last_known_monthly_fee,
        last_value(status_length, true) over w_recent as last_known_status_length
    from deduped
    window w_recent as (partition by customer_id order by start_date, id rows between unbounded preceding and current row)
),
deduped_subscriptions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by customer_id, plan order by monthly_fee desc nulls last, id) as rn
    from with_windows
    qualify rn = 1
)
-- Aggregate with conditional metrics
select
    customer_id,
    status,
    count_if(status = 'CANCELLED') as cancelled_events,
    sum(case when plan = 'standard' then monthly_fee_int else 0 end) as standard_monthly_fee_int,
    sum(case when plan = 'enterprise' then last_known_monthly_fee else 0 end) as enterprise_last_known_monthly_fee,
    count_if(plan = 'enterprise') as enterprise_events,
    sort_array(collect_set(status)) as status_list
from deduped_subscriptions
group by customer_id, status
having enterprise_last_known_monthly_fee >= 0
order by enterprise_events;
