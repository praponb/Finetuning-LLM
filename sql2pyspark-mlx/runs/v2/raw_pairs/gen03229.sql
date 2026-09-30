select * from (select *, rank() over (partition by customer_id order by discount desc) as row_num from orders where order_date >= '2024-10-01') as ranked where row_num <= 3 order by customer_id desc;
