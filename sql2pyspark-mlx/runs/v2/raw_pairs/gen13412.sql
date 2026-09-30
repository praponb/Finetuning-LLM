select *, round(cost * 0.85, 2) cost_adjusted from shipments where order_id is null and status = 'DELIVERED';
