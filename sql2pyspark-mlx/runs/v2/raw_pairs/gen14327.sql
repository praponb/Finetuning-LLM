select * from (select brand, category, price from products where price >= 300) pivot (avg(price) for category in ('Home', 'Sports', 'Fashion', 'Electronics'))
