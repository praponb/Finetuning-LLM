select status, round(avg(amount), 2) as avg_amount, round(avg(shipping_fee), 2) as avg_shipping_fee from orders where store_id in (13, 25, 28, 33) group by status having avg_shipping_fee > 80
