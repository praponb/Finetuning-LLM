select id, shipped_date from shipments where carrier in ('DHL', 'Flash', 'Kerry') union select id, shipped_date from shipments where carrier = 'Kerry' order by id
