with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        unit_price,
        coalesce(unit_price, 0) unit_price_filled
    from order_items
    where order_id = 35
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        unit_price,
        unit_price_filled,
        row_number() over w_hist event_seq,
        sum(unit_price) over w_running running_unit_price,
        row_number() over (order by unit_price, id) event_seq_2
    from cleaned
    window w_hist as (partition by order_id order by unit_price, id), w_running as (partition by order_id order by unit_price_filled, id rows between unbounded preceding and current row)
),
joined as (
    -- Attach orders attributes
    select
        src.*,
        o.order_date,
        o.status
    from metrics src
    left join orders o
        on src.order_id = o.id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lag(unit_price, 1) over (partition by order_id order by order_date, id) prev_unit_price,
        ntile(3) over (order by unit_price, id) unit_price_bucket
    from joined
)
-- Grouped metrics
select
    date_trunc('month', order_date) as activity_month,
    case when event_seq_2 >= 8 then 'PREMIUM' else 'STANDARD' end as event_seq_2_tier,
    round(avg(prev_unit_price), 2) as avg_prev_unit_price,
    sort_array(collect_set(status)) as status_list,
    count(distinct order_id) as distinct_order_ids,
    rank() over (partition by date_trunc('month', order_date) order by count(distinct order_id) desc) as distinct_order_ids_rank
from scored
group by date_trunc('month', order_date), case when event_seq_2 >= 8 then 'PREMIUM' else 'STANDARD' end;
