select discount, lower(status) as status_lower from orders where not (status in ('CANCELLED', 'PENDING', 'RETURNED') or customer_id = 36);
