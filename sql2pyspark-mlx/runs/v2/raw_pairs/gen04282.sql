select month(order_date) as order_month, sum(amount) as total_amount from orders group by 1 having total_amount < 8600
