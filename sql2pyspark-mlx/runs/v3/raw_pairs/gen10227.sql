select id, product_id, unit_price
from (
    select *, rank() over (partition by product_id order by unit_price) as row_num
    from order_items
) as t
where row_num <= 2
order by product_id;
