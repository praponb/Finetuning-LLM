select x.account_type, y.status from (select distinct account_type from accounts) x cross join (select distinct status from payments) y
