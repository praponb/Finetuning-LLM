select id, customer_id, shipping_fee, percent_rank() over (partition by customer_id order by shipping_fee desc) as shipping_fee_pct_rank from orders where shipping_fee <= 24;
