select a.id, a.status, a.amount from payments a where a.amount > (select avg(b.amount) from payments b where b.status = a.status);
