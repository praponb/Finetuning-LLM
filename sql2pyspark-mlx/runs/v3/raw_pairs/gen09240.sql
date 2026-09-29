select category, supplier_id, max(launch_date) as last_launch_date, avg(stock_qty) as avg_stock_qty from products group by category, supplier_id order by avg_stock_qty desc;
