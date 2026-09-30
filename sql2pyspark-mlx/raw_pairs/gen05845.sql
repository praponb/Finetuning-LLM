with base as (
    select *, date_sub(last_restock, 14) as last_restock_minus_14d
    from inventory
    where last_restock >= date '2023-07-24'
      and quantity = 99
),
kept_keys as (
    select id, store_id
    from base
    intersect
    select id, store_id
    from inventory
    where last_restock >= '2025-09-01'
),
reconciled as (
    select s.*
    from base s
    inner join kept_keys k on s.id = k.id and s.store_id = k.store_id
),
cleaned as (
    select
        id,
        product_id,
        quantity,
        last_restock,
        last_restock_minus_14d,
        date_add(last_restock, 42) as last_restock_plus_42d
    from reconciled
    where store_id in (1, 2, 5, 6)
),
enriched as (
    select
        e.*,
        d.product_name,
        d.stock_qty
    from cleaned e
    left join products d
        on e.product_id = d.id
),
daily as (
    select
        date_trunc('week', last_restock_plus_42d) as week_start,
        stddev_samp(quantity) as quantity_stddev,
        count(distinct id) as distinct_ids,
        round(avg(quantity), 2) as avg_quantity
    from enriched
    group by week_start
)
select
    week_start,
    quantity_stddev,
    distinct_ids
from daily
order by distinct_ids, week_start
