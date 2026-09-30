select status from shipments union select status from subscriptions where end_date < '2025-04-01';
