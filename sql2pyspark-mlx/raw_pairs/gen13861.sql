with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        date_format(paid_at, 'yyyy-MM-dd') paid_at_label,
        coalesce(method, 'N/A') resolved_method
    from payments
    where paid_at > date '2023-10-06'
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(amount) over w_hist order_id_min_amount,
        dense_rank() over (partition by month(paid_at), order_id order by amount nulls first) amount_dense_rank,
        sum(amount) over (partition by order_id order by amount, id rows between unbounded preceding and current row) running_amount
    from cleaned
    window w_hist as (partition by order_id)
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        b.channel,
        b.order_date
    from scored a
    left join orders b
        on a.order_id = b.id
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by order_id order by amount desc nulls last) as dedup_rank
        from with_dims
    ) x
    where dedup_rank = 1
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by channel order by running_amount desc, id) as dedup_rank_2
        from ranked
    ) ranked
    where dedup_rank_2 = 1
)
-- Join a dimension and aggregate
select
    o.channel as order_channel,
    o.status as order_status,
    src.method,
    count(*) as num_rows,
    min(src.amount) as min_amount,
    round(sum(src.order_id_min_amount), 2) as total_order_id_min_amount
from deduped src
left join orders o on src.order_id = o.id
group by grouping sets ((o.channel, o.status, src.method), (o.channel, o.status), (o.channel), ())
order by total_order_id_min_amount nulls last
