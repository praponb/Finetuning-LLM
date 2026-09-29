with unpacked as (
    -- Higher-order array functions
    select
        id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        attributes,
        attributes['gift'] as gift,
        try_cast(attributes['weight_kg'] as double) as weight_kg,
        array_join(array_sort(coupon_codes), ',') as coupons_text,
        size(attributes) as num_attributes
    from carts
)
-- Ranking and gap features
select
    id,
    coupons_text,
    percent_rank() over (order by weight_kg desc) as weight_kg_percent_rank,
    max(num_attributes) over w_part as num_attributes_max_2h,
    count(weight_kg) over w_part as weight_kg_count_2h
from unpacked
window w_part as (order by cast(created_at as bigint) range between 7200 preceding and current row);
