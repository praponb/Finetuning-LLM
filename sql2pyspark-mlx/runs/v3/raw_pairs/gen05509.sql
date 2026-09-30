select carrier, cost, delivered_date, case when carrier in ('FedEx', 'UPS') then 'Group A' else 'Other' end carrier_group, concat_ws('-', carrier, status) carrier_status_key from shipments;
