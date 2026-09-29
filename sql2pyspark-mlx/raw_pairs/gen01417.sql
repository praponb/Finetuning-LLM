select shipped_date as event_date from shipments union all select launch_date as event_date from products order by event_date;
