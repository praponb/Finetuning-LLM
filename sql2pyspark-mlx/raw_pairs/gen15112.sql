select x.status, y.carrier from (select distinct status from orders) x cross join (select distinct carrier from shipments) y order by status, carrier;
