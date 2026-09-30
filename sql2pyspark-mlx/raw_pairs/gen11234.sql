with latest_match as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.status,
        s.channel,
        x.shipment_cost,
        x.shipment_status
    from orders s
    left join lateral (
            select c.cost as shipment_cost, c.status as shipment_status
            from shipments c
            where c.order_id = s.id
            order by c.delivered_date desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.quantity < 7
        )
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by date_trunc('week', order_date), status order by shipment_cost, id) as row_seq,
        row_number() over (order by shipment_cost desc, id) as seq_no,
        avg(shipment_cost) over w_part as shipment_cost_avg_6,
        lead(shipment_cost, 2) over (partition by store_id order by shipment_cost, id) as next_shipment_cost,
        ntile(5) over w_hist as shipment_cost_bucket
    from eligible
    window w_part as (partition by customer_id order by shipment_cost, id rows between 6 preceding and current row), w_hist as (partition by status order by shipment_cost desc, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.order_id = 32
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.carrier not in ('FedEx', 'Kerry')
        )
)
select
    id,
    store_id,
    order_date,
    status,
    channel,
    shipment_cost,
    shipment_status,
    row_seq,
    shipment_cost_avg_6,
    case when channel rlike '^(web|store)$' then 'EXTENDED' when channel rlike '^(mobile|partner)$' then 'OTHER_ZONE' else 'CORE' end as channel_zone
from not_flagged
order by channel_zone desc;
