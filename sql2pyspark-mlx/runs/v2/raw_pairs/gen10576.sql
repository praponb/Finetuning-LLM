select id, delivered_date, shipped_date, status from shipments where delivered_date = (select max(delivered_date) from shipments) order by delivered_date desc;
