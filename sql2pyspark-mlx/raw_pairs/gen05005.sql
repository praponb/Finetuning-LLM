with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_day(last_restock) as last_restock_month_end
    from inventory
    where quantity != 101
),
enriched as (
    -- Attach products attributes
    select
        s.*,
        p.product_name,
        p.launch_date,
        p.cost
    from valid s
    join products p
        on s.product_id = p.id
    where p.product_name like '%Maker%'
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by quantity nulls last, id) as dedup_rank
        from enriched
    ) ranked
    where dedup_rank = 1
),
enriched_inventory as (
    -- Attach stores attributes
    select
        e.*,
        d.opened_date,
        d.region,
        d.city
    from deduped e
    left join stores d
        on e.store_id = d.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        last_restock,
        last_restock_month_end,
        launch_date,
        cost,
        dedup_rank,
        opened_date,
        region,
        city,
        lead(quantity) over w_running as next_quantity,
        lead(cost) over w_running as next_cost,
        row_number() over w_running as seq_no,
        first_value(quantity) ignore nulls over (partition by product_id order by quantity, id rows between unbounded preceding and current row) as first_known_quantity
    from enriched_inventory
    window w_running as (partition by product_id order by opened_date, id)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by city order by launch_date, id) as row_num
        from metrics
    ) t
    where row_num = 1
)
-- Ranking and gap features
select
    id,
    last_restock_month_end,
    launch_date,
    dedup_rank,
    next_quantity,
    next_cost,
    seq_no,
    row_num,
    sum(next_quantity) over (partition by city order by seq_no, id rows between unbounded preceding and current row) as running_next_quantity,
    sum(cost) over (partition by region order by launch_date, id rows between unbounded preceding and current row) as cumulative_cost,
    avg(cost) over (partition by date_trunc('month', last_restock), region order by last_restock_month_end, id rows between 6 preceding and current row) as cost_avg_6
from latest
