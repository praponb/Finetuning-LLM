with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(cost as int) as cost_int,
        cast(cost as int) as cost_int_2
    from shipments
    where order_id is not null
),
kept_keys as (
    -- Set intersection on keys
    select id, carrier
    from filtered
    intersect
    select id, carrier
    from shipments
    where carrier in ('DHL', 'Kerry')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.id = k.id and s.carrier = k.carrier
),
with_dims as (
    -- Attach orders attributes
    select
        a.order_id,
        a.delivered_date,
        a.cost,
        a.status,
        a.cost_int,
        a.cost_int_2,
        d.amount,
        d.order_date
    from verified a
    inner join orders d
        on a.order_id = d.id
    where d.status = 'COMPLETED'
),
scored as (
    -- Running and moving aggregates
    select
        order_id,
        delivered_date,
        cost,
        status,
        amount,
        max(cost_int) over w_ordered as status_max_cost_int
    from with_dims
    window w_ordered as (partition by status)
),
joined as (
    -- Attach orders attributes
    select
        s.order_id,
        s.delivered_date,
        s.status,
        s.amount,
        s.status_max_cost_int,
        b.discount,
        b.order_date
    from scored s
    left join orders b
        on s.order_id <=> b.id
),
scored_shipments as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by status, order_id order by amount desc) as amount_percent_rank,
        min(discount) over (partition by status, order_id) as status_min_discount
    from joined
)
select
    s.order_id,
    s.delivered_date,
    s.status,
    s.amount,
    s.discount,
    s.order_date,
    case when status rlike '^(IN_TRANSIT|RETURNED)$' then 'GROUP_A' when status rlike '^(DELIVERED|LOST)$' then 'OTHER_ZONE' else 'EXTENDED' end as status_zone,
    (select max(t2.cost) from shipments t2 where t2.order_id = s.order_id) as max_shipment_cost
from scored_shipments s
order by status desc;
