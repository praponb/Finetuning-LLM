select * from (select brand, category, price from products where stock_qty != 274) pivot (count(price) for category in ('Books', 'Home', 'Fashion')) order by brand;
