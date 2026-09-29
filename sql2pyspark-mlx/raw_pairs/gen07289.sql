with base as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        add_months(last_restock, 3) last_restock_plus_months,
        round(quantity, 0) quantity_rounded
    from inventory
    where quantity between 106 and 228
),
joined as (
    -- Attach stores attributes
    select e.*, d.city
    from base e
    left join stores d
        on e.store_id = d.id
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        last_value(quantity_rounded, true) over (partition by store_id order by quantity_rounded, id rows between unbounded preceding and current row) last_known_quantity_rounded,
        row_number() over (partition by product_id order by last_restock_plus_months desc, id) seq_no
    from joined
    where store_id = 3
),
kept_keys as (
    -- Set difference on keys
    select id, product_id
    from with_windows
    except distinct
    select id, product_id
    from inventory
    where product_id in (1, 2, 8, 10)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.id = k.id and s.product_id = k.product_id
)
-- Multi-level aggregation
select
    case when quantity >= 257 then 'LARGE' else 'SMALL' end as quantity_tier,
    date_trunc('week', last_restock) as week_start,
    round(sum(last_known_quantity_rounded) / nullif(count(case when city = 'London' then 1 end), 0), 2) as last_known_quantity_rounded_per_london,
    count(case when city = 'London' then 1 end) as london_count,
    sort_array(collect_set(city)) as city_list,
    size(collect_set(city)) as num_cities,
    round(sum(quantity) / nullif(count(case when city = 'Berlin' then 1 end), 0), 2) as quantity_per_berlin,
    count(*) as num_rows
from kept_rows
group by case when quantity >= 257 then 'LARGE' else 'SMALL' end, date_trunc('week', last_restock)
order by num_rows desc
