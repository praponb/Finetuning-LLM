with store_summary as (
    select store_id, count(shipping_fee) as shipping_fee_count
    from orders
    group by store_id
)
select s.id, s.city, ss.shipping_fee_count
from stores s
left outer join store_summary ss on s.id = ss.store_id
order by city desc
