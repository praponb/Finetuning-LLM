select city, count(*) as num_suppliers, avg(rating) as avg_rating, count(distinct rating) as unique_citys from suppliers where country = 'US' and city in ('Tokyo', 'Singapore') group by city;
