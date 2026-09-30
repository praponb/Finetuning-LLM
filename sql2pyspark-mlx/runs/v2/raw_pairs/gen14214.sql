select id, order_id, datediff(delivered_date, shipped_date) days_shipped_date_to_delivered_date from shipments where status in ('DELIVERED', 'IN_TRANSIT', 'LOST') or order_id is not null order by id;
