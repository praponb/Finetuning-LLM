SELECT *, FLOOR(cost) cost_whole FROM shipments WHERE status NOT IN ('RETURNED', 'IN_TRANSIT', 'DELIVERED');
