select start_date event_date from subscriptions where start_date is not null union all select delivered_date event_date from shipments where delivered_date is not null order by event_date;
