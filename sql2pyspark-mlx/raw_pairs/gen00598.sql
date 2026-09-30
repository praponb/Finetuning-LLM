with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from stores s
    where exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.order_date < s.opened_date
        )
)
select
    id,
    store_name,
    region,
    opened_date
from kept
order by id desc
