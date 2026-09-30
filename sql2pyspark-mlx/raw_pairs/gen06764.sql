with unpacked as (
    -- Higher-order array functions
    select
        id,
        customer_id,
        created_at,
        items,
        attributes,
        array_contains(map_keys(attributes), 'weight_kg') as has_weight_kg,
        coalesce(attributes['gift'], 'unknown') as gift_or_default,
        ship_to.city as ship_city,
        exists(coupon_codes, c -> c like 'W%') as has_welcome_code,
        attributes['utm_source'] as utm_source
    from carts
    where created_at < '2023-06-05'
)
-- Ranking and gap features
select
    *,
    last_value(ship_city) ignore nulls over (partition by customer_id, ship_city order by created_at asc, id asc rows between unbounded preceding and current row) as last_known_ship_city,
    row_number() over (partition by ship_city order by created_at asc, id asc) as event_seq
from unpacked;
