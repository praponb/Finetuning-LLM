select case when c.age >= 62 then 'high' when c.age >= 29 then 'mid' else 'low' end age_bucket, avg(o.discount) avg_discount, max(o.order_date) latest_order_date
from orders o
full outer join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
group by case when c.age >= 62 then 'high' when c.age >= 29 then 'mid' else 'low' end
