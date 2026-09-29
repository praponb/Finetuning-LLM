select a.id, a.status, a.cost from shipments a where a.cost = (select max(b.cost) from shipments b where b.status = a.status);
