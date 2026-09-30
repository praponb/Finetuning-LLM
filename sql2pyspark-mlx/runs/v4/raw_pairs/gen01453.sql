select *, add_months(order_date, 6) as order_date_plus_months, round(discount, 1) as discount_rounded from orders where store_id = 23 and discount > 0.18 and customer_id = 30
