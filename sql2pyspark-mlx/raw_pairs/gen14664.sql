select 'shipment' as source, status from shipments where year(delivered_date) >= 2023 union select 'subscription' as source, status from subscriptions where customer_id in (8, 12, 17, 34);
