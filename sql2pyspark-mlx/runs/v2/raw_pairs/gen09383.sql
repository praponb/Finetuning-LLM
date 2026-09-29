select cost, cast(cost as int) as cost_int from shipments where order_id is not null and status = 'DELIVERED' order by cost_int asc;
