with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        quantity,
        dayofweek(last_restock) as last_restock_dayofweek,
        round(quantity * 1.15, 2) as quantity_scaled
    from inventory
    where quantity between 85 and 101
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by store_id, product_id order by quantity_scaled desc, id) as rank_in_group
        from valid
    ) ranked
    where rank_in_group = 1
),
remaining_keys as (
    -- Set intersection on keys
    select store_id, product_id
    from ranked
    intersect all
    select store_id, product_id
    from inventory
    where quantity between 70 and 160
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join remaining_keys k on s.store_id = k.store_id and s.product_id = k.product_id
),
matched_keys as (
    -- Set intersection on keys
    select store_id, product_id
    from kept_rows
    intersect
    select store_id, product_id
    from inventory
    where quantity <> 154
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    inner join matched_keys k on s.store_id = k.store_id and s.product_id = k.product_id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        count(last_restock_dayofweek) over (partition by product_id, store_id) as product_id_count_last_restock_dayofweek,
        rank() over (order by last_restock_dayofweek nulls first) as last_restock_dayofweek_rank,
        cume_dist() over (partition by product_id order by quantity desc) as quantity_cume_dist
    from reconciled
)
-- Aggregate with conditional metrics
select
    case when last_restock_dayofweek >= 81 then 'LARGE' else 'SMALL' end as last_restock_dayofweek_tier,
    store_id,
    product_id,
    bit_and(quantity) as quantity_bit_and,
    round(avg(quantity), 2) as avg_quantity,
    approx_percentile(product_id_count_last_restock_dayofweek, array(0.25, 0.5, 0.75)) as product_id_count_last_restock_dayofweek_quartiles,
    grouping_id() as grouping_id
from enriched
group by cube (case when last_restock_dayofweek >= 81 then 'LARGE' else 'SMALL' end, store_id, product_id)
having bit_and(quantity) < 1000
order by avg_quantity;
