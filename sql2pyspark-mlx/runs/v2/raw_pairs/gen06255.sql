SELECT DISTINCT channel, status FROM orders WHERE status NOT IN ('SHIPPED', 'RETURNED') ORDER BY status DESC;
