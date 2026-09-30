select id, channel, discount, last_day(order_date) as order_date_month_end from orders where discount <= 0.26 or discount <= 0.37
