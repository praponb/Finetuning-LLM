select a.status, b.method from (select distinct status from accounts) a cross join (select distinct method from payments) b
