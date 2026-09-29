with cleaned as (
    -- Basic cleansing and defaults
    select *, upper(city) as city_upper
    from stores
    where year(opened_date) >= 2023
)
-- Latest matching order per row (LATERAL ... LIMIT 1)
select
    s.id,
    s.store_name,
    s.city,
    s.opened_date,
    s.sqft,
    top1.discount
from cleaned s
join lateral (
        select c.discount
        from orders c
        where c.store_id = s.id
        order by c.shipping_fee desc, c.id desc
        limit 1
    ) top1 on true;
