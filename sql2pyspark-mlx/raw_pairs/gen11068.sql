with latest as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        delivered_date,
        cost
    from shipments
    qualify rank() over (partition by status, carrier order by delivered_date nulls first) = 1
),
enriched as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost,
        d.amount,
        d.order_date
    from latest src
    inner join orders d
        on src.order_id = d.id
    where d.channel in ('partner', 'web')
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by order_id order by cost desc nulls first) cost_dense_rank,
        sum(amount) over (order by order_date, id rows between unbounded preceding and current row) running_amount,
        sum(cost) over (partition by order_id order by order_date, id rows between unbounded preceding and current row) running_cost,
        first_value(amount) ignore nulls over (order by amount, id rows between unbounded preceding and current row) first_known_amount,
        stddev_samp(cost) over (partition by order_id) order_id_stddev_samp_cost
    from enriched
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        case when running_amount >= 8518 then 'High' when running_amount >= 1749 then 'Medium' else 'Low' end running_amount_tier,
        cost + order_id_stddev_samp_cost cost_plus_order_id_stddev_samp_cost
    from scored
    where delivered_date >= to_date('2023-09-10')
      and order_id is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        min(amount) over (partition by order_id) order_id_min_amount,
        row_number() over (partition by order_id order by amount desc, id) event_seq
    from base
)
select
    id,
    cost,
    amount,
    cost_dense_rank,
    running_amount,
    first_known_amount,
    running_amount_tier,
    cost_plus_order_id_stddev_samp_cost,
    order_id_min_amount
from metrics
order by order_id_min_amount desc;
