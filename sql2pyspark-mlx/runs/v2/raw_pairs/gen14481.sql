select a.account_type, b.method from (select distinct account_type from accounts) a cross join (select distinct method from payments) b order by account_type, method
