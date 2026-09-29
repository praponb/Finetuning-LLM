with base as (
    -- Basic cleansing and defaults
    select
        *,
        trim(status) as clean_status,
        coalesce(method, 'DIRECT') as method_filled
    from payments
    where paid_at > date '2023-07-17'
      and order_id is not null
),
joined as (
    -- Attach orders attributes
    select e.*, p.shipping_fee
    from base e
    left join orders p
        on e.order_id = p.id
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        clean_status,
        method_filled,
        shipping_fee,
        sum(shipping_fee) over (partition by status order by amount, id rows between unbounded preceding and current row) as cumulative_shipping_fee,
        dense_rank() over (partition by status order by shipping_fee nulls last) as shipping_fee_dense_rank,
        sum(shipping_fee) over (partition by status order by cast(paid_at as bigint) range between 2592000 preceding and current row) as shipping_fee_sum_30d
    from joined
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        method,
        cumulative_shipping_fee,
        shipping_fee_dense_rank,
        row_number() over w_running as seq_no,
        sum(shipping_fee_dense_rank) over (partition by method) as method_sum_shipping_fee_dense_rank,
        sum(amount) over (partition by method, order_id order by cast(paid_at as bigint) range between 2592000 preceding and current row) as amount_sum_30d,
        count(cumulative_shipping_fee) over w_recent as cumulative_shipping_fee_count_30d,
        row_number() over (partition by month(paid_at) order by shipping_fee, id) as row_seq
    from enriched
    window w_running as (partition by order_id order by shipping_fee_sum_30d desc, id), w_recent as (partition by order_id, method order by cast(paid_at as bigint) range between 2592000 preceding and current row)
)
-- Join a dimension and aggregate
select
    d.channel,
    s.method,
    count(*) as rows_total,
    max(s.method_sum_shipping_fee_dense_rank) as max_method_sum_shipping_fee_dense_rank,
    grouping_id() as grouping_level
from scored s
join orders d on s.order_id = d.id
group by grouping sets ((d.channel, s.method), (d.channel))
order by channel desc nulls first;
