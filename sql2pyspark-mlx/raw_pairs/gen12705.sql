with base as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        round(datediff(delivered_date, shipped_date) / 30, 1) as months_shipped_date_to_delivered_date,
        concat_ws('-', status, carrier) as status_carrier_key,
        coalesce(carrier, 'OTHER') as carrier_clean
    from shipments
    where shipped_date > date '2023-09-05'
      and carrier is not null
),
with_dims as (
    select
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.delivered_date,
        src.cost,
        src.carrier_clean,
        b.discount
    from base src
    join orders b
        on src.order_id = b.id
),
cleaned as (
    select
        *,
        date_add(delivered_date, 2) as delivered_date_plus_2d,
        case when discount >= 0.36 then 'High' when discount >= 0.08 then 'Medium' else 'Low' end as discount_level,
        coalesce(carrier, 'N/A') as resolved_carrier
    from with_dims
    where delivered_date > date '2023-05-26'
      and order_id is not null
),
with_dims_shipments as (
    select
        src.*,
        d.status,
        d.amount,
        d.channel
    from cleaned src
    join orders d
        on src.order_id = d.id
),
deduped as (
    select *
    from with_dims_shipments
    qualify row_number() over (partition by status order by amount desc, id) = 1
)
select
    date_trunc('year', shipped_date) as year_start,
    round(sum(cost) / nullif(count(case when status = 'COMPLETED' then 1 end), 0), 2) as cost_per_completed,
    count(*) filter (where channel = 'mobile') as mobile_rows,
    approx_percentile(discount, 0.5) as discount_p50,
    count(distinct carrier) as distinct_carriers
from deduped
group by date_trunc('year', shipped_date)
having discount_p50 >= 0
order by cost_per_completed desc;
