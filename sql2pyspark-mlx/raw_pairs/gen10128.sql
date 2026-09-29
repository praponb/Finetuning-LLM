select city, supplier_name from suppliers where (country in ('TH', 'DE', 'UK') and city in ('Singapore', 'New York', 'Berlin')) or city = 'Berlin'
