with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_sub(order_date, 41) as order_date_minus_41d,
        case when channel in ('web', 'partner') then 'Core' else 'Other' end as channel_group,
        coalesce(channel, 'UNKNOWN') as channel_clean
    from orders
    where order_date > to_date('2023-04-06')
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(amount) over w_ordered as cumulative_amount,
        row_number() over (partition by store_id order by order_date_minus_41d, id) as event_seq,
        first_value(discount) ignore nulls over (partition by status order by amount, id rows between unbounded preceding and current row) as first_known_discount,
        row_number() over (partition by status order by amount desc, id) as row_seq
    from valid
    window w_ordered as (partition by status order by order_date_minus_41d, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach customers attributes
    select
        src.*,
        c.country,
        c.credit_limit
    from metrics src
    left join customers c
        on src.customer_id = c.id
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_dims s
    where exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.shipped_date < '2024-03-01'
        )
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount) over (order by shipping_fee, id rows between unbounded preceding and current row) as running_discount,
        lead(first_known_discount, 1, 0) over (order by order_date_minus_41d, id) as next_first_known_discount,
        first_value(discount, true) over (partition by channel order by credit_limit, id rows between unbounded preceding and current row) as first_known_discount_2,
        lag(amount, 1) over (partition by status order by cumulative_amount, id) as prev_amount,
        sum(event_seq) over (partition by customer_id, status order by cumulative_amount, id rows between unbounded preceding and current row) as running_event_seq
    from kept
),
totals as (
    -- Grouped metrics
    select
        date_trunc('month', order_date) as activity_month,
        count(case when status = 'RETURNED' then 1 end) as returned_count,
        count(case when status = 'PENDING' then 1 end) as pending_count,
        sum(prev_amount) filter (where channel = 'partner') as partner_total_prev_amount,
        grouping(date_trunc('month', order_date)) as gid
    from with_windows
    group by cube (date_trunc('month', order_date))
)
select
    activity_month,
    returned_count,
    partner_total_prev_amount
from totals
order by activity_month
