select year(order_date) order_year, count(case when channel = 'partner' then 1 end) partner_count, avg(amount) avg_amount from orders group by year(order_date) order by order_year desc
