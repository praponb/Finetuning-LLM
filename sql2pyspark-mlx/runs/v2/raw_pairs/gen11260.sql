select id, delivered_date, order_id, case when carrier in ('Flash', 'UPS') then 'Tier 1' else 'Other' end as carrier_group from shipments order by carrier_group
