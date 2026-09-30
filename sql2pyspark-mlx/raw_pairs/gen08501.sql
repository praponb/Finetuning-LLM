with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        upper(channel) as channel_upper,
        case when discount >= 0.32 then 'High' when discount >= 0.19 then 'Medium' else 'Low' end as discount_tier
    from orders
    where order_date > date '2023-07-02'
      and channel is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        store_id,
        channel,
        amount,
        channel_upper,
        discount_tier,
        case when channel in ('web', 'mobile') then 'Group A' else 'Other' end as channel_group,
        coalesce(channel, 'UNKNOWN') as resolved_channel
    from filtered
    where store_id = 19
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lead(amount) over w_part as next_amount,
        lag(amount) over w_part as prev_amount,
        row_number() over w_part as event_seq
    from cleaned
    window w_part as (partition by store_id, customer_id order by amount, id)
),
joined as (
    -- Attach stores attributes
    select
        src.*,
        d.city,
        d.store_name
    from metrics src
    join stores d
        on src.store_id = d.id
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        discount_tier,
        channel_group,
        resolved_channel,
        prev_amount,
        event_seq,
        city,
        avg(next_amount) over (partition by customer_id order by next_amount, id rows between 2 preceding and current row) as next_amount_avg_2,
        lead(event_seq, 1) over w_running as next_event_seq
    from joined
    where city in ('Singapore', 'Berlin')
    window w_running as (order by amount, id)
)
-- Window metrics over several different partitions
select
    id,
    customer_id,
    resolved_channel,
    prev_amount,
    event_seq,
    city,
    next_event_seq,
    first_value(next_amount_avg_2) ignore nulls over (partition by customer_id, city order by event_seq, id rows between unbounded preceding and current row) as first_known_next_amount_avg_2,
    lag(next_amount_avg_2, 2) over (order by prev_amount, id) as prev_next_amount_avg_2
from enriched
