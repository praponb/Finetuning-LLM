select a.status, b.plan from (select distinct status from orders) a cross join (select distinct plan from subscriptions) b order by status, plan;
