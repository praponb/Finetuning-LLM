select id, order_date, store_id, shipping_fee, percent_rank() over (partition by month(order_date), store_id order by shipping_fee) as shipping_fee_pct_rank from orders order by shipping_fee desc
