select a.status, b.carrier from (select distinct status from payments) a cross join (select distinct carrier from shipments) b order by status, carrier;
