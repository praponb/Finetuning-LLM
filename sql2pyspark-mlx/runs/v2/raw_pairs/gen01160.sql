select status, 'shipment' as source from shipments union all select status, 'subscription' as source from subscriptions
