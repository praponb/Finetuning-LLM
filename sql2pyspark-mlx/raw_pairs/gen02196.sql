with flattened_attrs as (
    select
        id,
        customer_id,
        attributes,
        ship_to.city as ship_city,
        try_cast(attributes['weight_kg'] as double) as weight_kg,
        array_contains(coupon_codes, 'WELCOME5') as used_welcome5,
        coalesce(ship_to.zip, 'UNKNOWN') as zip_or_unknown,
        coalesce(attributes['gift'], 'unknown') as gift_or_default
    from carts
    where customer_id = 6
)
select
    id,
    customer_id,
    weight_kg,
    used_welcome5,
    zip_or_unknown,
    gift_or_default,
    sum(weight_kg) over w_running as last7_sum_weight_kg,
    count(weight_kg) over w_running as last7_count_weight_kg,
    min(weight_kg) over w_running as last7_min_weight_kg,
    avg(weight_kg) over w_running as last7_avg_weight_kg,
    lead(weight_kg, 1) over (partition by customer_id order by weight_kg, id) as next_weight_kg
from flattened_attrs
window w_running as (partition by ship_city, customer_id order by weight_kg, id rows between 6 preceding and current row);
