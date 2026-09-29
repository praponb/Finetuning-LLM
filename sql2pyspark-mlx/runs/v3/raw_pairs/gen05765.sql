select shipped_date from shipments where status in ('LOST', 'DELIVERED', 'RETURNED') intersect select launch_date as shipped_date from products where supplier_id is null
