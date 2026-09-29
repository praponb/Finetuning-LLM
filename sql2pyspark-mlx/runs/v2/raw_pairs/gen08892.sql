select x.channel, y.carrier from (select distinct channel from orders) x cross join (select distinct carrier from shipments) y order by channel, carrier
