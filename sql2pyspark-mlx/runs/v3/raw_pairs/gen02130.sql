select status, carrier, avg(cost) as avg_cost, min(cost) as min_cost from shipments where status = 'LOST' and carrier is null group by status, carrier;
