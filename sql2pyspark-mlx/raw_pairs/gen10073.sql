select a.id, a.status, a.cost, a.delivered_date from shipments a where a.cost > (select avg(b.cost) from shipments b where b.status = a.status)
