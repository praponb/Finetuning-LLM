with ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        store_id,
        product_id,
        units
    from (
        select *, row_number() over (partition by product_id order by sale_date desc nulls last, id) as latest_rank
        from store_sales
    ) ranked
    where latest_rank = 1
),
remaining_keys as (
    -- Set difference on keys
    select id, product_id
    from ranked
    except all
    select id, product_id
    from store_sales
    where product_id in (2, 4, 5, 9)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join remaining_keys k on s.id = k.id and s.product_id = k.product_id
),
kept_keys as (
    -- Set intersection on keys
    select product_id, id
    from verified
    intersect all
    select product_id, id
    from store_sales
    where product_id in (3, 10)
),
verified_store_sales as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join kept_keys k on s.product_id = k.product_id and s.id = k.id
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        round(units / sum(units) over (partition by product_id), 4) as units_share,
        sum(units) over (partition by product_id) as product_id_sum_units,
        round(units / sum(units) over (partition by store_id), 4) as units_share_2,
        avg(units) over (partition by product_id) as product_id_avg_units,
        cume_dist() over (partition by product_id, store_id order by units desc nulls last) as units_cume_dist
    from verified_store_sales
)
select
    id,
    store_id,
    units_share,
    product_id_avg_units
from with_windows
order by units_share
