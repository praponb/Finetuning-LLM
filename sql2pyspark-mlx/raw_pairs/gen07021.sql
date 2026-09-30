with base as (
    -- Basic cleansing and defaults
    select
        *,
        case when revenue >= 3500 then 'High' when revenue >= 1400 then 'Medium' else 'Low' end as revenue_level,
        cast(units as double) as units_num
    from store_sales
    where units < 23
),
joined as (
    -- Attach products attributes
    select
        e.*,
        b.category,
        b.launch_date
    from base e
    inner join products b
        on e.product_id = b.id
),
joined_store_sales as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.units,
        src.revenue,
        src.units_num,
        src.launch_date,
        b.launch_date as product_launch_date,
        b.price
    from joined src
    left join products b
        on src.product_id = b.id
    where b.price >= 580
),
remaining_keys as (
    -- Set difference on keys
    select id
    from joined_store_sales
    except
    select id
    from store_sales
    where units > 19
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined_store_sales s
    inner join remaining_keys k on s.id = k.id
)
select
    s.id,
    s.store_id,
    s.units,
    s.revenue,
    s.units_num,
    s.launch_date,
    s.product_launch_date,
    s.price,
    (select min(i2.quantity) from inventory i2 where i2.store_id = s.store_id) as min_inventory_quantity
from reconciled s
order by revenue desc nulls last;
