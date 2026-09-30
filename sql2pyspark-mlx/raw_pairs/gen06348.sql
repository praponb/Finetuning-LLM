with base as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        datediff(current_date(), last_restock) as days_since_last_restock
    from inventory
    where store_id = 4
),
filtered as (
    select
        *,
        least(quantity, days_since_last_restock) as least_quantity_days_since_last_restock,
        days_since_last_restock - quantity as days_since_last_restock_minus_quantity
    from base
    where last_restock >= to_date('2023-09-29')
),
deduped as (
    select *
    from filtered
    qualify rank() over (partition by product_id order by days_since_last_restock_minus_quantity) = 1
),
enriched as (
    select
        src.id,
        src.store_id,
        src.product_id,
        src.quantity,
        src.days_since_last_restock,
        src.days_since_last_restock_minus_quantity,
        b.sqft,
        b.city
    from deduped src
    left join stores b
        on src.store_id = b.id
    where b.opened_date <= '2023-12-01'
)
select
    s.id,
    s.store_id,
    s.quantity,
    s.days_since_last_restock,
    s.days_since_last_restock_minus_quantity,
    s.city,
    case when city rlike '^(Chiang Mai|Tokyo)$' then 'CORE' when city rlike '^(Bangkok|New York)$' then 'GROUP_A' else 'EXTENDED' end as city_zone,
    (select max(t2.quantity) from inventory t2 where t2.store_id = s.store_id) as max_inventory_quantity
from enriched s;
