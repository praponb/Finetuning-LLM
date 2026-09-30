with valid as (
    -- Keep recent, valid rows
    select *, length(store_name) as store_name_length
    from stores
    where region = 'Central'
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, x.quantity
    from valid s
    left join lateral (
            select c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
base as (
    -- Basic cleansing and defaults
    select *, sqft - store_name_length as sqft_minus_store_name_length
    from with_latest
    where city not in ('London', 'Chiang Mai')
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    store_name_length,
    quantity,
    case when city rlike '^(London|Berlin)$' then 'OTHER_ZONE' when city rlike '^(Bangkok|Chiang Mai)$' then 'CORE' else 'GROUP_B' end as city_zone
from base
order by sqft desc nulls first;
