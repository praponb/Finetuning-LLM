select id, country, rating, case when city = 'Singapore' then 1 else 0 end is_singapore, trim(country) clean_country from suppliers
