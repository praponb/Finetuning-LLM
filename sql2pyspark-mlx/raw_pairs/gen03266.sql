select o.status, year(o.order_date) as order_year, count(distinct s.cost) as unique_carriers
from shipments s
left join orders o on s.order_id = o.id
where o.shipping_fee between 22 and 35
group by o.status, year(o.order_date)
order by order_year desc, unique_carriers desc;
