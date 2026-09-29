with enriched as (
    select
        id,
        order_id,
        carrier,
        delivered_date,
        status,
        ntile(3) over w_recent as cost_bucket,
        percent_rank() over (order by cost nulls first) as cost_percent_rank
    from shipments
    window w_recent as (partition by carrier order by cost, id)
),
deduped as (
    select *, rank() over (partition by status order by delivered_date) as row_num
    from enriched
    qualify row_num = 1
),
with_dims as (
    select
        a.id,
        a.order_id,
        a.carrier,
        a.delivered_date,
        a.status,
        a.cost_bucket,
        a.cost_percent_rank,
        b.channel,
        b.discount,
        b.status as order_status
    from deduped a
    inner join orders b
        on a.order_id = b.id
),
matched_keys as (
    select order_id, carrier
    from with_dims
    except
    select order_id, carrier
    from shipments
    where status = 'LOST'
),
verified as (
    select s.*
    from with_dims s
    inner join matched_keys k on s.order_id = k.order_id and s.carrier = k.carrier
),
enriched_shipments as (
    select
        id,
        order_id,
        cost_bucket,
        max(discount) over (partition by order_status) as order_status_max_discount,
        dense_rank() over (partition by carrier, order_id order by cost_bucket nulls last) as cost_bucket_dense_rank,
        avg(discount) over (partition by channel, carrier) as channel_avg_discount,
        percent_rank() over (partition by carrier order by cost_bucket desc) as cost_bucket_percent_rank
    from verified
    where channel = 'web'
),
grouped as (
    select
        order_id,
        approx_percentile(order_status_max_discount, 0.95) as order_status_max_discount_p95,
        count(*) as row_count,
        approx_percentile(cost_bucket_dense_rank, array(0.25, 0.5, 0.75)) as cost_bucket_dense_rank_quartiles,
        count(distinct order_id) as distinct_order_ids,
        round(avg(order_status_max_discount), 2) as avg_order_status_max_discount,
        min(channel_avg_discount) as min_channel_avg_discount
    from enriched_shipments
    group by order_id
    having count(distinct order_id) > 1
)
select
    order_id,
    cost_bucket_dense_rank_quartiles,
    distinct_order_ids,
    avg_order_status_max_discount,
    min_channel_avg_discount
from grouped
order by order_id nulls last;
