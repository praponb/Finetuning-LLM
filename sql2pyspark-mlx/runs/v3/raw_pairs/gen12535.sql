select status, count(discount) as discount_count from orders where status not in ('SHIPPED', 'COMPLETED') and shipping_fee < 15 group by status
