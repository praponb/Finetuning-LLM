select c.country, round((100 * sum(case when o.status = 'SHIPPED' then 1 else 0 end)) / count(*), 1) as pct_shipped, round(stddev(o.shipping_fee), 2) as stddev_shipping_fee
from orders o
left join customers c on c.id = o.customer_id
left join stores s on o.store_id = s.id
where o.customer_id in (4, 35)
group by c.country
having round((100 * sum(case when o.status = 'SHIPPED' then 1 else 0 end)) / count(*), 1) > 20;
