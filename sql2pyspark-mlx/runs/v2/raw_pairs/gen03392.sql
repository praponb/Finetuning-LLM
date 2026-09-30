select a.id, a.status, a.discount, a.order_date from orders a where a.discount < (select avg(b.discount) from orders b where b.status = a.status)
