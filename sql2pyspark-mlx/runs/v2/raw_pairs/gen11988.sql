select id, brand, launch_date, stock_qty, lead(stock_qty, 1) over (partition by brand order by launch_date, id) next_stock_qty from products;
