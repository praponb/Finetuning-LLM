select p.id, p.status, p.amount, p.method from payments p where p.amount = (select max(p2.amount) from payments p2 where p2.status = p.status) order by p.amount desc;
