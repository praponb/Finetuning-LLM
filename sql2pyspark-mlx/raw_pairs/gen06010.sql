select 'account' as record_type, opened_date from accounts where account_type = 'LOAN' union select 'shipment' as record_type, delivered_date from shipments order by opened_date
