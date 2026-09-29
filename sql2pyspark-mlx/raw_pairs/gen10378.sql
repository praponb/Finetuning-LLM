with cleaned as (
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        case when rating >= 3.8 then 'High' when rating >= 1.4 then 'Medium' else 'Low' end rating_level,
        coalesce(city, 'DIRECT') resolved_city
    from suppliers
    where city is not null
)
select s.*
from cleaned s
where not exists (
        select 1
        from products ch
        where ch.supplier_id = s.id
          and ch.price is null
    )
