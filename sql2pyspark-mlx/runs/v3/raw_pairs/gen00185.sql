select
    quantity_bucket,
    product_id,
    total_quantity,
    avg_quantity
from (
    select
        case when quantity >= 8 then 'high' when quantity >= 2 then 'mid' else 'low' end as quantity_bucket,
        product_id,
        sum(quantity) as total_quantity,
        avg(quantity) as avg_quantity
    from order_items
    group by case when quantity >= 8 then 'high' when quantity >= 2 then 'mid' else 'low' end, product_id
) as s
where total_quantity < 30
order by product_id desc;
