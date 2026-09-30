select id, cost, status, shipped_date from shipments where cost > (select avg(cost) from shipments) order by status desc;
