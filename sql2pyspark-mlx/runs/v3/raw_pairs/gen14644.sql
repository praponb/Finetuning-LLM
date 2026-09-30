select * from (select brand, category, cost from products) pivot (count(cost) for category in ('Sports', 'Home')) order by brand
