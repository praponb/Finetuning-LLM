select carrier, status, count(cost) as cost_count, min(cost) as min_cost from shipments group by carrier, status order by carrier, status;
