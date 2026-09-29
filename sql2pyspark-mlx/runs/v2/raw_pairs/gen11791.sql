select status, 'payment' as source from payments where order_id is not null union all select status, 'shipment' as source from shipments where year(delivered_date) >= 2024 order by source;
