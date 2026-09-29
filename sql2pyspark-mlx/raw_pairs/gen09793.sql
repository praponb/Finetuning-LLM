select s.id, s.status, s.cost, s.delivered_date from shipments s where s.cost = (select max(s2.cost) from shipments s2 where s2.status = s.status);
