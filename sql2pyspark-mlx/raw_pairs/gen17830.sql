with with_dims as (
    -- Attach orders attributes
    select
        a.*,
        o.shipping_fee,
        o.discount,
        o.amount
    from order_items a
    left join orders o
        on a.order_id = o.id
),
joined as (
    -- Attach products attributes
    select
        a.id,
        a.product_id,
        a.unit_price,
        a.discount,
        a.amount,
        d.product_name,
        d.category,
        d.price
    from with_dims a
    join products d
        on a.product_id = d.id
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by price, id) as rank_in_group
    from joined
    qualify rank_in_group = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(price, true) over w_grp as first_known_price,
        lead(amount, 1) over (order by discount, id) as next_amount,
        last_value(discount, true) over (partition by category order by price, id rows between unbounded preceding and current row) as last_known_discount
    from latest
    where discount < 0.28
    window w_grp as (partition by product_id order by rank_in_group, id rows between unbounded preceding and current row)
),
enriched as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        s.unit_price,
        s.discount,
        s.category,
        s.next_amount,
        d.brand,
        d.cost,
        d.stock_qty
    from with_windows s
    left join products d
        on s.product_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        avg(cost) over (partition by product_id order by discount, id rows between 5 preceding and 1 preceding) as cost_avg_5_prior,
        last_value(brand, true) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) as last_known_brand,
        avg(discount) over (partition by category, brand) as category_avg_discount
    from enriched
)
select
    id,
    product_id,
    unit_price,
    next_amount,
    brand,
    cost_avg_5_prior,
    last_known_brand,
    category_avg_discount,
    case when brand rlike '^(Initech|Umbrella)$' then 'GROUP_A' when brand rlike '^(Acme|Globex)$' then 'CORE' else 'EXTENDED' end as brand_zone
from scored
order by brand
