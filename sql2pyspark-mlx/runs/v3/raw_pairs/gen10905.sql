with store_summary as (
    select store_id, max(order_date) as last_order_date
    from orders
    where amount >= 790
    group by store_id
)
select s.opened_date, ss.last_order_date
from stores s
left join store_summary ss on s.id = ss.store_id;
