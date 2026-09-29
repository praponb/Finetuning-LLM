with scored as (
    -- Running and moving aggregates
    select
        *,
        lead(quantity, 1) over (partition by store_id order by last_restock, id) as next_quantity,
        sum(quantity) over (partition by date_trunc('WEEK', last_restock), store_id order by quantity, id rows between unbounded preceding and current row) as cumulative_quantity,
        first_value(quantity) ignore nulls over w_ordered as first_known_quantity
    from inventory
    window w_ordered as (partition by product_id order by last_restock, id rows between unbounded preceding and current row)
),
joined as (
    -- Attach stores attributes
    select s.*, b.region
    from scored s
    left join stores b
        on s.store_id = b.id
    where b.region is null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        lead(quantity, 1) over w_grp as next_quantity_2,
        lead(next_quantity, 2) over (partition by store_id, product_id order by quantity, id) as next_next_quantity,
        lag(cumulative_quantity, 1) over (partition by product_id order by last_restock, id) as prev_cumulative_quantity,
        first_value(quantity) ignore nulls over (partition by store_id order by cumulative_quantity, id rows between unbounded preceding and current row) as first_known_quantity_2,
        first_value(next_quantity, true) over w_grp_2 as first_known_next_quantity
    from joined
    window w_grp as (partition by store_id, product_id order by next_quantity, id), w_grp_2 as (partition by month(last_restock) order by first_known_quantity, id rows between unbounded preceding and current row)
),
joined_inventory as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.prev_cumulative_quantity,
        e.first_known_next_quantity,
        p.region,
        p.city,
        p.sqft
    from with_windows e
    left join stores p
        on e.store_id = p.id
)
-- Join a dimension and aggregate
select
    dim.region as store_region,
    s.city as inventory_city,
    count(*) as cnt,
    min(s.prev_cumulative_quantity) as min_prev_cumulative_quantity,
    avg(s.first_known_next_quantity) as avg_first_known_next_quantity
from joined_inventory s
join stores dim on s.store_id = dim.id
group by grouping sets ((dim.region, s.city), (dim.region))
order by min_prev_cumulative_quantity nulls last
