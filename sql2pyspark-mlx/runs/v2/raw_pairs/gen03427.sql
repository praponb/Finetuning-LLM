select id, status, store_id, discount - shipping_fee as discount_minus_shipping_fee from orders where status in ('COMPLETED', 'CANCELLED') and store_id is not null and status = 'SHIPPED'
