select status, least(discount, shipping_fee) as least_discount_shipping_fee from orders where order_date < '2023-09-18' and store_id in (4, 39) order by status
