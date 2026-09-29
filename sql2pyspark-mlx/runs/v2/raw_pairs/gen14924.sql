select id, is_active, lower(country) as country_lower from customers where segment = 'Consumer' order by id desc limit 10
