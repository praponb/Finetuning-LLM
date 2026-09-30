SELECT MONTH(order_date) AS order_month, SUM(shipping_fee) AS total_shipping_fee, RANK() OVER (ORDER BY SUM(shipping_fee) DESC) AS shipping_fee_rank FROM orders GROUP BY MONTH(order_date)
