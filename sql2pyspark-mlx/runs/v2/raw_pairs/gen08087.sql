with StoreSummary as (
    select store_id, min(order_date) as first_order_date
    from orders
    where discount < 0.13
    group by store_id
)
select s.id, s.opened_date, S.first_order_date
from stores s
left join StoreSummary S on s.id = S.store_id
