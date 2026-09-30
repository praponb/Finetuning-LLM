with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        case when city in ('London', 'Berlin') then 'Group A' else 'Other' end city_group,
        last_day(opened_date) opened_date_month_end,
        coalesce(region, 'NONE') region_filled
    from stores
    where opened_date >= date '2023-05-11'
)
-- Deduplicate with ROW_NUMBER
select
    id,
    store_name,
    region,
    opened_date,
    city_group,
    region_filled
from valid
qualify row_number() over (partition by city order by opened_date_month_end nulls first, id) = 1;
