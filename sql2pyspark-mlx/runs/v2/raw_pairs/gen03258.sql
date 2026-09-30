select id, shipped_date, cost, order_id from shipments where shipped_date >= (select max(shipped_date) from shipments where cost < 62) order by cost desc, order_id desc;
