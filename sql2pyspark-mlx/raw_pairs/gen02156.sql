select *, row_number() over (partition by order_id order by unit_price desc nulls last, id) as latest_rank from order_items qualify latest_rank = 1
