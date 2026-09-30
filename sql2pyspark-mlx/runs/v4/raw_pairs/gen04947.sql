with parsed as (
    -- Semi-structured parsing
    select
        id,
        created_at,
        aggregate(items, 0D, (acc, item) -> acc + item.qty * item.price, acc -> round(acc, 2)) as cart_value,
        coalesce(attributes['utm_source'], 'none') as utm_source_or_default,
        ship_to.city as ship_city,
        array_contains(map_keys(attributes), 'utm_source') as has_utm_source,
        size(items) as num_items
    from carts
)
-- Grouped metrics
select
    ship_city,
    date_trunc('day', created_at) as activity_day,
    count(case when ship_city = 'Bangkok' then 1 end) as bangkok_count,
    sort_array(collect_set(ship_city)) as ship_city_list,
    round(avg(cart_value), 2) as avg_cart_value,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    avg(cart_value) as avg_cart_value_2,
    approx_percentile(num_items, 0.95) as num_items_p95
from parsed
group by ship_city, activity_day
order by ship_city
