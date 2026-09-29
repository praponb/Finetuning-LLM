select * from (select category, brand, price from products)
pivot (sum(price) for brand in ('Acme', 'Umbrella', 'Initech', 'Globex'))
order by category
