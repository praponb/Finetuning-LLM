select status from orders where discount >= 0.17 union all select status from orders where status not in ('COMPLETED', 'CANCELLED', 'SHIPPED')
