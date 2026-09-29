select a.id, a.status, a.amount, a.paid_at from payments a where a.amount > (select avg(b.amount) from payments b where b.status = a.status) order by a.status desc, a.amount desc
