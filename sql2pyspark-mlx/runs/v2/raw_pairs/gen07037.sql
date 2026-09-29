select
    s.id,
    s.sqft,
    s.city,
    t.pending_count,
    t.avg_shipping_fee
from stores s
left join (select store_id, sum(case when status = 'PENDING' then 1 else 0 end) as pending_count, round(avg(shipping_fee), 2) as avg_shipping_fee
    from orders
    where order_date <= '2024-07-01'
    group by store_id) t on s.id = t.store_id
