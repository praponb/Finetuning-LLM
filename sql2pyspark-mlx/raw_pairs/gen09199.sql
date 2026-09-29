select id, shipping_fee from orders where (discount < 0.15 and amount between 1190 and 1270) or status = 'CANCELLED'
