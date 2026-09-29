with metrics as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        carrier,
        cost,
        status,
        dense_rank() over (partition by status order by cost asc) as cost_dense_rank,
        first_value(carrier) ignore nulls over (partition by carrier, order_id order by cost asc, id asc rows between unbounded preceding and current row) as first_known_carrier,
        avg(cost) over (partition by month(delivered_date), carrier order by cost asc, id asc rows between 3 preceding and 1 preceding) as cost_avg_3_prior
    from shipments
),
joined as (
    -- Attach orders attributes
    select s.*, d.status as order_status
    from metrics s
    left join orders d
        on s.order_id = d.id
    where d.amount >= 1070
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        o.status as order_status_2,
        o.discount,
        o.shipping_fee
    from joined src
    left join orders o
        on src.order_id = o.id
),
remaining_keys as (
    -- Set difference on keys
    select status
    from with_dims
    except distinct
    select status
    from shipments
    where month(delivered_date) in (5, 6, 7)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    join remaining_keys k on s.status = k.status
)
-- Join a dimension and aggregate
select
    p.status as order_status,
    a.order_status_2,
    count(*) as cnt,
    round(avg(a.cost_avg_3_prior), 2) as avg_cost_avg_3_prior,
    grouping_id() as gid
from verified a
join orders p on a.order_id = p.id
group by grouping sets ((p.status, a.order_status_2), (p.status))
order by order_status asc;
