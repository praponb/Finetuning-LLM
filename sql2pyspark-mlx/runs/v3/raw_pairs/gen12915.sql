select channel from orders where order_date >= '2023-04-01' union select channel from orders where customer_id is null;
