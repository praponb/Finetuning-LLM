select s.id, s.status, s.cost from shipments s where s.cost < (select avg(s2.cost) from shipments s2 where s2.status = s.status);
