with scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by order_id order by quantity desc nulls last) as quantity_percent_rank,
        corr(unit_price, quantity) over (partition by product_id) as corr_unit_price_quantity,
        row_number() over (order by unit_price desc, id) as event_seq
    from order_items
),
remaining_keys as (
    -- Set difference on keys
    select product_id
    from scored
    except all
    select product_id
    from order_items
    where order_id = 21
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    join remaining_keys k on s.product_id = k.product_id
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        p.shipping_fee,
        p.channel,
        p.status
    from verified a
    left join orders p
        on a.order_id = p.id
),
enriched as (
    -- Attach products attributes
    select
        e.*,
        d.brand,
        d.launch_date
    from with_dims e
    join products d
        on e.product_id = d.id
)
-- Aggregate with conditional metrics
select
    case when corr_unit_price_quantity >= 0.74 then 'PREMIUM' else 'STANDARD' end as corr_unit_price_quantity_tier,
    min(shipping_fee) as min_shipping_fee,
    bit_and(quantity) as quantity_bit_and,
    count(*) as row_count
from enriched
group by 1
order by quantity_bit_and desc;
