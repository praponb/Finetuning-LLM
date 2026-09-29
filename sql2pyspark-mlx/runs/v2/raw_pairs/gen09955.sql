select id, brand, launch_date, stock_qty, lag(stock_qty, 1) over (partition by brand order by launch_date, id) prev_stock_qty from products
