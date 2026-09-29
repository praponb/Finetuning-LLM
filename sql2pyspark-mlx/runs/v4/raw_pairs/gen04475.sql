select * from (select city, country, rating from suppliers)
pivot (sum(rating) for country in ('JP', 'TH', 'SG'));
