select c.country, sum(s.cost) total_cost from shipments s join orders o on s.order_id = o.id left join customers c on o.customer_id = c.id group by c.country order by country desc;
