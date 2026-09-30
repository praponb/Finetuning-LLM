select s.region, round(avg(o.discount), 2) as avg_discount from orders o left outer join stores s on o.store_id = s.id inner join customers c on o.customer_id = c.id group by s.region
