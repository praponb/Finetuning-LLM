select * from (select id, brand, stock_qty, price, rank() over (partition by brand order by stock_qty desc) row_num from products) ranked where row_num <= 5;
