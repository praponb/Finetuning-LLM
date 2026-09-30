select id, order_id, shipped_date from shipments where shipped_date >= '2024-02-01' union all select id, order_id, shipped_date from shipments where delivered_date <= '2023-09-01';
