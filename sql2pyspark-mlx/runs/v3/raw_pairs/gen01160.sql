select status, 'payment' as source from payments union all select status, 'shipment' as source from shipments order by source
