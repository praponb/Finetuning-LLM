select d.channel, src.carrier, count(*) as cnt, min(src.cost) as min_cost from shipments src left join orders d on src.order_id = d.id group by d.channel, src.carrier order by cnt nulls first;
