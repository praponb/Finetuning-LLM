select country, sum(rating) as total_rating from suppliers where supplier_name like 'Eur%' and country in ('DE', 'JP') group by country having sum(rating) >= 14 order by country
