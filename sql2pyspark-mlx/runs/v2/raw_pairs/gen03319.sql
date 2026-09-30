select distinct channel from orders where status not in ('CANCELLED', 'PENDING');
