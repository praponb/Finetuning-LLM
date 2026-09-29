with with_dims as (
    -- Attach orders attributes
    select s.*, b.channel
    from order_items s
    left join orders b
        on s.order_id = b.id
    where b.shipping_fee is not null
)
-- Grouped metrics
select
    case when unit_price >= 320 then 'LARGE' else 'SMALL' end as unit_price_tier,
    count_if(channel = 'web') as web_events,
    size(collect_set(channel)) as num_channels,
    size(collect_set(channel)) as num_channels_2,
    sort_array(collect_set(channel)) as channel_list
from with_dims
group by case when unit_price >= 320 then 'LARGE' else 'SMALL' end
order by web_events desc nulls first;
