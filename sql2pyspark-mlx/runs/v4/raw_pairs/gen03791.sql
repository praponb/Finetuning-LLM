with enriched as (
    -- Attach products attributes
    select
        s.*,
        d.product_name,
        d.launch_date
    from order_items s
    join products d
        on s.product_id = d.id
)
select
    id,
    product_id,
    unit_price,
    product_name,
    launch_date,
    case when product_name rlike '^(USB-C Hub|Backpack XL)$' then 'OTHER_ZONE' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'CORE' else 'EXTENDED' end as product_name_zone
from enriched
order by id desc;
