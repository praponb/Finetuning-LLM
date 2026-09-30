select status, 'shipment' as source from shipments where cost between 66 and 71 union all select status, 'subscription' as source from subscriptions where plan = 'standard' order by source desc
