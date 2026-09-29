select p.id, p.status, p.amount from payments p where p.amount = (select min(p2.amount) from payments p2 where p2.status = p.status);
