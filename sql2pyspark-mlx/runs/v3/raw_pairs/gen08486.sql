select a.channel, b.carrier from (select distinct channel from orders) a cross join (select distinct carrier from shipments) b;
