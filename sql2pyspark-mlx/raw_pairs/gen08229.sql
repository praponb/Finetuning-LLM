select
    id,
    order_id,
    unit_price,
    product_id,
    dense_rank() over (partition by order_id order by unit_price desc) as unit_price_dense_rank,
    row_number() over (partition by product_id order by unit_price, id) as unit_price_row
from order_items
where quantity between 6 and 7;
