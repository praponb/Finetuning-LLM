select * from order_items qualify rank() over (partition by product_id order by quantity desc nulls first) = 1
