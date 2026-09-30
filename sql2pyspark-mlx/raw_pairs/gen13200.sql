select *
from (
    select *, row_number() over (partition by product_id order by quantity, id) as rnk
    from order_items
    where quantity = 7
) as t
where rnk <= 1
