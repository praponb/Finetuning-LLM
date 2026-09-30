select status from orders where status in ('COMPLETED', 'PENDING', 'CANCELLED') intersect select status from subscriptions order by status;
