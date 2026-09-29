select id, status, discount, min(discount) over (partition by status) as status_min_discount from orders where discount >= 0.2
