select *, stock_qty * cost stock_qty_times_cost, round(cost / price, 2) margin from products where month(launch_date) in (3, 4, 11);
