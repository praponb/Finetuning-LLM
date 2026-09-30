with scored as (
    select
        *,
        count(cost) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as running_count_cost,
        avg(cost) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as running_avg_cost,
        sum(cost) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as running_sum_cost
    from shipments
    where status = 'LOST'
),
joined as (
    select e.*, b.shipping_fee
    from scored e
    left join orders b
        on e.order_id = b.id
),
with_dims as (
    select
        e.*,
        p.amount,
        p.status as order_status
    from joined e
    left join orders p
        on e.order_id = p.id
),
scored_shipments as (
    select
        id,
        carrier,
        running_avg_cost,
        shipping_fee,
        amount,
        max(shipping_fee) over (partition by order_status, status order by running_sum_cost, id rows between unbounded preceding and current row) as running_max_shipping_fee,
        min(shipping_fee) over (partition by order_status, status order by running_sum_cost, id rows between unbounded preceding and current row) as running_min_shipping_fee,
        count(shipping_fee) over (partition by order_status, status order by running_sum_cost, id rows between unbounded preceding and current row) as running_count_shipping_fee,
        cume_dist() over (partition by status, order_id order by amount desc) as amount_cume_dist
    from with_dims
),
latest as (
    select *
    from scored_shipments
    qualify rank() over (partition by carrier order by running_avg_cost desc nulls first) = 1
)
select
    id,
    running_avg_cost,
    shipping_fee,
    ntile(5) over w_part as running_max_shipping_fee_bucket,
    round(running_avg_cost / sum(running_avg_cost) over (partition by carrier), 4) as running_avg_cost_share,
    avg(running_count_shipping_fee) over w_ordered as running_count_shipping_fee_avg_6_prior
from latest
window w_part as (partition by carrier order by running_max_shipping_fee, id), w_ordered as (partition by carrier order by running_max_shipping_fee, id rows between 6 preceding and 1 preceding);
