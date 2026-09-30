select id, delivered_date, date_add(delivered_date, 43) as delivered_date_plus_43d from shipments order by delivered_date_plus_43d desc;
