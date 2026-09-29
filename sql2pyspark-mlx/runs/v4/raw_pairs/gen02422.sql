select x.status, y.method from (select distinct status from accounts) x cross join (select distinct method from payments) y;
