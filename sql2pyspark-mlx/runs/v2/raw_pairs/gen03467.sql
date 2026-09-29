select status, 'shipment' as source from shipments union all select status, 'subscription' as source from subscriptions where year(end_date) = 2025 order by status
