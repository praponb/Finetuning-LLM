select 'shipment' as source, shipped_date from shipments where month(delivered_date) in (2, 5, 8) union select 'subscription' as source, start_date from subscriptions order by shipped_date desc;
