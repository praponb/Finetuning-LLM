select category, round(avg(price), 2) as avg_price, round(avg(stock_qty), 2) as avg_stock_qty from products group by category
