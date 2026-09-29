select x.account_type, y.plan from (select distinct account_type from accounts) x cross join (select distinct plan from subscriptions) y
