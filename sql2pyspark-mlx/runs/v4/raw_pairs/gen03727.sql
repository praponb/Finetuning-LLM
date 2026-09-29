with enriched as (
    select
        src.*,
        o.status as order_status,
        o.amount,
        o.shipping_fee
    from shipments src
    left join orders o
        on src.order_id = o.id
),
with_windows as (
    select
        *,
        first_value(order_status, true) over (partition by order_id order by shipping_fee, id rows between unbounded preceding and current row) first_known_order_status,
        row_number() over (partition by order_id, carrier order by shipping_fee, id) row_seq,
        sum(cost) over (partition by status order by shipped_date, id rows between unbounded preceding and current row) running_cost,
        ntile(4) over (order by shipping_fee desc, id) shipping_fee_bucket
    from enriched
),
enriched_shipments as (
    select
        a.*,
        d.status as order_status_2,
        d.discount,
        d.channel
    from with_windows a
    inner join orders d
        on a.order_id = d.id
),
kept_keys as (
    select order_id, carrier
    from enriched_shipments
    except all
    select order_id, carrier
    from shipments
    where delivered_date < '2024-12-01'
),
kept_rows as (
    select s.*
    from enriched_shipments s
    inner join kept_keys k on s.order_id = k.order_id and s.carrier = k.carrier
),
valid as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        order_status,
        amount,
        shipping_fee,
        row_seq,
        running_cost,
        discount,
        cast(amount as int) amount_int,
        coalesce(carrier, 'NONE') resolved_carrier
    from kept_rows
    where delivered_date >= date '2023-04-29'
),
scored as (
    select
        *,
        count(cost) over w_grp order_id_count_cost,
        rank() over w_hist discount_rank,
        rank() over (order by shipping_fee desc) shipping_fee_rank
    from valid
    window w_grp as (partition by order_id, order_status), w_hist as (partition by order_status order by discount desc nulls last)
),
joined as (
    select a.*, p.status
    from scored a
    left join orders p
        on a.order_id = p.id
)
select
    case when shipping_fee >= 36 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
    order_status,
    count(distinct carrier) as distinct_carriers,
    count_if(order_status = 'SHIPPED') as shipped_events,
    sum(case when carrier = 'UPS' then discount else 0 end) as ups_discount,
    stddev_samp(cost) as cost_stddev
from joined
group by case when shipping_fee >= 36 then 'PREMIUM' else 'STANDARD' end, order_status;
