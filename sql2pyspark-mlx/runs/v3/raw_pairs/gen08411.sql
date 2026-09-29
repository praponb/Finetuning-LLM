select * from (select city, country, rating from suppliers where country in ('UK', 'US', 'TH'))
pivot (max(rating) for country in ('SG', 'JP', 'UK'))
