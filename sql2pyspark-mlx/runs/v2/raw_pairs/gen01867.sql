select * from (select brand, category, price from products) pivot (avg(price) for category in ('Grocery', 'Home', 'Books')) order by brand
