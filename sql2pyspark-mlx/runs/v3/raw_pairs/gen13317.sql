select a.id, a.status, a.cost, a.order_id from shipments a where a.cost < (select avg(b.cost) from shipments b where b.status = a.status) order by a.id;
