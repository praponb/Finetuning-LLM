select id, delivered_date, cost, shipped_date from shipments where delivered_date <= (select min(delivered_date) from shipments) order by shipped_date asc, id asc limit 5
