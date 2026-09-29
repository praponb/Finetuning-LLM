with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        datediff(end_date, start_date) as days_start_date_to_end_date
    from subscriptions
    where end_date > date '2023-05-22'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify row_number() over (partition by status order by monthly_fee desc, id) = 1
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        customer_id,
        plan,
        start_date
    from (
        select *, rank() over (partition by customer_id order by days_start_date_to_end_date desc nulls first) as latest_rank
        from deduped
    ) t
    where latest_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by plan order by start_date, id) as event_seq,
        row_number() over (partition by customer_id order by start_date desc, id) as row_seq
    from ranked
)
-- Grouped metrics
select
    customer_id,
    avg(event_seq) as avg_event_seq,
    sum(case when plan = 'standard' then event_seq else 0 end) as standard_event_seq,
    count_if(plan = 'standard') as standard_events
from metrics
group by customer_id;
