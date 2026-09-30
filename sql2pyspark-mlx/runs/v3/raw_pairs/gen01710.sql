select x.account_type, y.method from (select distinct account_type from accounts) x cross join (select distinct method from payments) y;
