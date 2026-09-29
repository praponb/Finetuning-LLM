select x.status, y.plan from (select distinct status from shipments) x cross join (select distinct plan from subscriptions) y;
