with valid as (
    -- Basic cleansing and defaults
    select
        *,
        round(unit_price * 0.9, 2) as unit_price_scaled,
        case when quantity >= 9 then 'High' when quantity >= 3 then 'Medium' else 'Low' end as quantity_tier
    from order_items
    where product_id is not null
      and quantity > 7
),
remaining_keys as (
    -- Set difference on keys
    select id
    from valid
    except distinct
    select id
    from order_items
    where order_id = 6
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.id = k.id
),
matched_keys as (
    -- Set intersection on keys
    select product_id, id
    from verified
    intersect
    select product_id, id
    from order_items
    where unit_price >= 230
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join matched_keys k on s.product_id = k.product_id and s.id = k.id
),
with_dims as (
    -- Attach products attributes
    select
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        d.brand,
        d.category
    from kept_rows src
    left join products d
        on src.product_id = d.id
)
-- Join a dimension and aggregate
select
    p.channel,
    src.brand,
    count(*) as cnt,
    min(src.quantity) as min_quantity
from with_dims src
inner join orders p on src.order_id = p.id
group by p.channel, src.brand;
