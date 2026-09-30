select a.status, b.method from (select distinct status from orders) a cross join (select distinct method from payments) b;
