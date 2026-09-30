select * from (select category, brand, price from products)
pivot (avg(price) for brand in ('Stark', 'Umbrella'))
order by category;
