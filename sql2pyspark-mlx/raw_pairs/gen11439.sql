with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        substring(method, 1, 4) as method_prefix,
        coalesce(method, 'NONE') as resolved_method
    from payments
    where paid_at >= timestamp '2023-09-04 03:00:00'
),
joined as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.paid_at,
        e.status,
        e.method_prefix,
        p.order_date,
        p.channel,
        p.shipping_fee
    from cleaned e
    left join orders p
        on e.order_id <=> p.id
    where p.amount >= 820
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        status,
        order_date,
        channel,
        shipping_fee
    from (
        select *, row_number() over (partition by status, order_id order by paid_at desc, id) as rank_in_group
        from joined
    ) t
    where rank_in_group = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        status,
        order_date,
        channel
    from (
        select *, row_number() over (partition by status order by order_date desc, id) as dedup_rank
        from deduped
    ) ranked
    where dedup_rank = 1
),
latest_payments as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by status order by order_date) as row_num
    from latest
    qualify row_num = 1
)
-- Grouped metrics
select
    status,
    count(distinct status) as distinct_status,
    sort_array(collect_set(channel)) as channel_list,
    size(collect_set(channel)) as num_channels,
    sort_array(collect_set(status)) as status_list
from latest_payments
group by status
order by status desc
