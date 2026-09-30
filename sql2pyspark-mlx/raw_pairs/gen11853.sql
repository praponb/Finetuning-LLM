with top_order_items as (
    select *, row_number() over (partition by product_id order by unit_price, id) as rn
    from order_items
)
select *
from top_order_items
where rn <= 1
order by quantity desc
