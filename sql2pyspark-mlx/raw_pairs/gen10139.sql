select x.channel, y.status from (select distinct channel from orders) x cross join (select distinct status from subscriptions) y order by channel, status;
