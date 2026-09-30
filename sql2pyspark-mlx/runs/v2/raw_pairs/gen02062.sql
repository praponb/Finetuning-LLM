select id, delivered_date, status from shipments where delivered_date = (select min(delivered_date) from shipments) order by status;
