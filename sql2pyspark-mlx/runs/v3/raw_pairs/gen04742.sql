select a.id, a.status, a.cost, a.shipped_date from shipments a where a.cost = (select min(b.cost) from shipments b where b.status = a.status) order by a.cost desc, a.shipped_date
