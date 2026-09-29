select * from (select city, country, rating from suppliers) pivot (count(rating) for country in ('JP', 'TH', 'UK'))
