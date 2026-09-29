select status, 'shipment' source from shipments union select status, 'subscription' source from subscriptions order by status desc;
