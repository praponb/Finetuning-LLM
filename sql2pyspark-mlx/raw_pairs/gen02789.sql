select a.status, b.plan from (select distinct status from accounts) a cross join (select distinct plan from subscriptions) b;
