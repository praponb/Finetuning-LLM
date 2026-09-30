select date_format(launch_date, 'yyyy-MM') as launch_month, category, max(cost) as max_cost from products where stock_qty = 351 group by date_format(launch_date, 'yyyy-MM'), category;
