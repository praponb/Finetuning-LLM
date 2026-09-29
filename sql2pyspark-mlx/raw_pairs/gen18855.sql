select id, status, discount, order_date from (select *, row_number() over (partition by status order by discount desc, id) as row_num from orders) as ranked where row_num <= 2;
