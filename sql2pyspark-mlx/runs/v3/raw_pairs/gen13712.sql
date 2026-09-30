select channel, max(discount) as max_discount, count(*) as cnt from orders where order_date > '2023-04-10' and status not in ('CANCELLED', 'RETURNED', 'COMPLETED') group by channel;
