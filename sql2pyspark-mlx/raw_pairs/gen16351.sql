select * from (select category, brand, stock_qty from products) pivot (count(stock_qty) for brand in ('Initech', 'Stark', 'Globex')) order by category;
