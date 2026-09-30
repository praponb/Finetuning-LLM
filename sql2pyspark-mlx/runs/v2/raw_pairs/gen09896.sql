select x.carrier, y.status from (select distinct carrier from shipments) x cross join (select distinct status from subscriptions) y order by carrier, status
