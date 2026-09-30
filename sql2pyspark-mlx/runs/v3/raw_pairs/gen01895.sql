select o.channel, round(avg(s.cost), 2) avg_cost from shipments s inner join orders o on o.id = s.order_id group by o.channel order by avg_cost desc, channel;
