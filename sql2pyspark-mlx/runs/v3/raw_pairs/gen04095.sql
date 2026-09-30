select x.channel, y.plan from (select distinct channel from orders) x cross join (select distinct plan from subscriptions) y;
