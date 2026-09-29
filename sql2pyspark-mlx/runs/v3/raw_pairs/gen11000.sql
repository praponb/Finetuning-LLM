select status, 'account' as source from accounts union all select status, 'shipment' as source from shipments order by source desc;
