select id, order_date, status from orders where order_date >= (select max(order_date) from orders) order by order_date
