select shipping_fee, amount from orders where status not in ('SHIPPED', 'COMPLETED', 'RETURNED') order by shipping_fee desc;
