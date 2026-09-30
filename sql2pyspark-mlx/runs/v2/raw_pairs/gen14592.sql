select city from suppliers where city = 'New York' union all select city from suppliers where country in ('TH', 'US', 'DE')
