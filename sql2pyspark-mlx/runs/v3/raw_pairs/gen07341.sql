select brand, min(stock_qty) as min_stock_qty, max(cost) as max_cost from products where price between 720 and 1140 group by 1;
