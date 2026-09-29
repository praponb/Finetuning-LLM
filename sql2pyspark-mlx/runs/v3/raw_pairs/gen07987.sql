select stores.region, count(orders.shipping_fee) as shipping_fee_count, round((100 * sum(case when orders.status = 'SHIPPED' then 1 else 0 end)) / count(*), 1) as pct_shipped
from orders
inner join stores on orders.store_id = stores.id
full outer join customers on orders.customer_id = customers.id
where orders.discount >= 0.2
group by stores.region
order by shipping_fee_count desc
