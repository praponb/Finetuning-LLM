select status, count(distinct discount) as unique_customers, min(order_date) as min_order_date from orders group by status having count(distinct discount) >= 5 order by min_order_date;
