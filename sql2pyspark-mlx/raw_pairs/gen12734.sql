with parsed as (
    -- Semi-structured parsing
    select
        id,
        customer_id,
        coupon_codes,
        size(attributes) as num_attributes,
        array_max(transform(items, i -> i.price)) as max_item_price,
        exists(items, i -> i.price > 100) as has_premium_item
    from carts
)
-- Ranking and gap features
select
    id,
    coupon_codes,
    max_item_price,
    avg(num_attributes) over w_part as num_attributes_avg_2_prior,
    rank() over (partition by customer_id order by num_attributes desc nulls last) as num_attributes_rank
from parsed
where max_item_price >= 150
window w_part as (partition by customer_id order by max_item_price, id rows between 2 preceding and 1 preceding)
