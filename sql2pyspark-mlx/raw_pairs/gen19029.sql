select status, hour(paid_at) paid_at_hour from payments where (status <> 'REFUNDED' and method <> 'cash') or order_id in (12, 16) order by status;
