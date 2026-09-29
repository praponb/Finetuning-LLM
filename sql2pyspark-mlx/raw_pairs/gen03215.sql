with enriched as (
    -- Attach suppliers attributes
    select
        a.id,
        a.product_name,
        a.category,
        a.brand,
        a.stock_qty,
        a.launch_date,
        a.tags,
        a.supplier_id,
        s.supplier_name
    from products a
    left join suppliers s
        on a.supplier_id = s.id
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        category,
        brand,
        launch_date,
        tags
    from enriched
    qualify row_number() over (partition by category order by launch_date, id) = 1
),
remaining_keys as (
    -- Set difference on keys
    select brand, category
    from latest
    except all
    select brand, category
    from products
    where length(product_name) < 13
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join remaining_keys k on s.brand = k.brand and s.category = k.category
),
remaining_keys_2 as (
    -- Set difference on keys
    select brand
    from verified
    except
    select brand
    from products
    where launch_date between '2023-05-01' and '2025-05-17'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    join remaining_keys_2 k on s.brand = k.brand
),
remaining_keys_3 as (
    -- Set difference on keys
    select id, brand
    from kept_rows
    except
    select id, brand
    from products
    where cost between 270 and 510
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    join remaining_keys_3 k on s.id = k.id and s.brand = k.brand
)
-- Multi-level aggregation
select
    brand,
    count_if(category = 'Grocery') as grocery_events,
    count(distinct category) as distinct_categories,
    count(*) filter (where category = 'Electronics') as electronics_rows
from reconciled
group by brand
order by brand desc nulls first;
