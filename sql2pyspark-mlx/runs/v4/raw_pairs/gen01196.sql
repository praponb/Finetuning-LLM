select * from order_items qualify row_number() over (partition by product_id order by unit_price desc nulls last, id) = 1
