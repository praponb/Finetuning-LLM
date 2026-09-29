select country, city from suppliers where country != 'SG' union select country, city from suppliers where country not in ('UK', 'DE')
