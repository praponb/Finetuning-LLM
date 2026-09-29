with parsed as (
    -- Higher-order array functions
    select
        id,
        customer_id,
        created_at,
        ship_to,
        attributes,
        ship_to.country as ship_country,
        try_cast(attributes['weight_kg'] as double) as weight_kg
    from carts
)
-- Grouped metrics
select
    customer_id,
    date_trunc('day', created_at) as activity_day,
    count_if(ship_country = 'UK') as uk_events,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    sort_array(collect_set(ship_country)) as ship_country_list,
    dense_rank() over (partition by customer_id order by count_if(ship_country = 'UK') desc) as uk_events_dense_rank
from parsed
group by customer_id, date_trunc('day', created_at)
