with matched_keys as (
    -- Set intersection on keys
    select id, product_id
    from order_items
    intersect
    select id, product_id
    from order_items
    where unit_price > 130
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from order_items s
    inner join matched_keys k on s.id = k.id and s.product_id = k.product_id
),
enriched as (
    -- Attach orders attributes
    select e.*, o.shipping_fee
    from kept_rows e
    left join orders o
        on e.order_id = o.id
),
enriched_order_items as (
    -- Attach products attributes
    select
        a.*,
        d.brand,
        d.category
    from enriched a
    left join products d
        on a.product_id = d.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        round(quantity * 1.1, 2) as adjusted_quantity,
        coalesce(brand, 'UNKNOWN') as brand_clean
    from enriched_order_items
    where product_id is not null
),
matched_keys_2 as (
    -- Set difference on keys
    select product_id, order_id
    from filtered
    except
    select product_id, order_id
    from order_items
    where unit_price between 230 and 340
),
kept_rows_order_items as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys_2 k on s.product_id = k.product_id and s.order_id = k.order_id
),
joined as (
    -- Attach orders attributes
    select
        a.*,
        b.status,
        b.amount
    from kept_rows_order_items a
    left join orders b
        on a.order_id = b.id
)
-- Aggregate with conditional metrics
select
    case when adjusted_quantity >= 62 then 'PREMIUM' else 'STANDARD' end as adjusted_quantity_tier,
    brand,
    stddev_samp(quantity) as quantity_stddev,
    sum(case when category = 'Fashion' then unit_price else 0 end) as fashion_unit_price,
    sum(quantity) filter (where status = 'SHIPPED') as shipped_total_quantity,
    round(sum(shipping_fee) / nullif(count(case when brand = 'Stark' then 1 end), 0), 2) as shipping_fee_per_stark,
    sum(shipping_fee) as total_shipping_fee,
    grouping_id() as gid
from joined
group by rollup (case when adjusted_quantity >= 62 then 'PREMIUM' else 'STANDARD' end, brand)
order by brand
