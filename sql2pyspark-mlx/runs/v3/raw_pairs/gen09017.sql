select start_date from subscriptions where month(start_date) in (1, 2, 12) except select delivered_date from shipments where carrier = 'Flash' order by start_date;
