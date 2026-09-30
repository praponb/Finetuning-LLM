with parsed as (
    -- Higher-order array functions
    select
        id,
        customer_id,
        items,
        attributes,
        array_max(transform(items, item -> item.price)) as max_item_price,
        size(filter(items, item -> item.qty >= 3)) as bulk_lines,
        coalesce(attributes['utm_source'], 'direct') as utm_source_or_default,
        try_cast(attributes['weight_kg'] as double) as weight_kg,
        size(items) as num_items
    from carts
)
-- Running and moving aggregates
select
    id,
    max_item_price,
    bulk_lines,
    utm_source_or_default,
    weight_kg,
    num_items,
    min(weight_kg) over w_running as last7_min_weight_kg,
    avg(weight_kg) over w_running as last7_avg_weight_kg,
    max(weight_kg) over w_running as last7_max_weight_kg,
    count(num_items) over w_running_2 as customer_id_count_num_items
from parsed
window w_running as (partition by customer_id order by weight_kg asc, id asc rows between 6 preceding and current row), w_running_2 as (partition by customer_id)
