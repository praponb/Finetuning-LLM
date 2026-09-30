select status, quarter(order_date) as order_quarter, max(order_date) as latest_order_date, min(amount) as min_amount from orders group by status, quarter(order_date)
