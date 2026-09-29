select status, 'account' as source from accounts where customer_id = 39 union select status, 'shipment' as source from shipments where cost >= 41
