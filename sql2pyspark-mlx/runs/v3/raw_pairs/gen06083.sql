select id, delivered_date from shipments where delivered_date = (select max(delivered_date) from shipments)
