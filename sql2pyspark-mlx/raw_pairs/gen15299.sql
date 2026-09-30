with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        date_format(last_restock, 'MMM yyyy') as last_restock_label
    from inventory
    where last_restock >= date '2023-02-08'
),
joined as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        d.sqft
    from filtered s
    left join stores d
        on s.store_id = d.id
),
with_dims as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        b.sqft as store_sqft,
        b.region,
        b.city
    from joined src
    left join stores b
        on src.store_id = b.id
    where year(b.opened_date) = 2024
),
joined_inventory as (
    -- Attach stores attributes
    select s.*, b.region as store_region
    from with_dims s
    inner join stores b
        on s.store_id = b.id
)
-- Grouped metrics
select
    city,
    case when store_sqft >= 16799 then 'PREMIUM' else 'STANDARD' end as store_sqft_tier,
    store_id,
    sort_array(collect_set(city)) as city_list,
    sum(case when store_region = 'North' then store_sqft else 0 end) as north_store_sqft,
    count_if(store_region = 'East') as east_events,
    size(collect_set(store_region)) as num_store_regions
from joined_inventory
group by grouping sets ((city, case when store_sqft >= 16799 then 'PREMIUM' else 'STANDARD' end, store_id), (city, case when store_sqft >= 16799 then 'PREMIUM' else 'STANDARD' end), ())
order by city desc nulls last;
