select delivered_date from shipments where month(delivered_date) in (2, 7, 8) intersect select start_date from subscriptions where plan = 'premium'
