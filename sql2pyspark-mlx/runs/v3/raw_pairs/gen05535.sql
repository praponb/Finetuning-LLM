select s.id, s.status, s.cost, s.shipped_date from shipments s where s.cost > (select avg(s2.cost) from shipments s2 where s2.status = s.status) order by s.id desc, s.cost desc
