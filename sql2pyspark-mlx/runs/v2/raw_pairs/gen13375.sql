select * from (select city, country, rating from suppliers) pivot (count(rating) for country in ('SG', 'DE', 'US', 'TH')) order by city
