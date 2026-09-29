select city from suppliers where country in ('SG', 'TH', 'DE') union select city from suppliers where rating >= 3.0
