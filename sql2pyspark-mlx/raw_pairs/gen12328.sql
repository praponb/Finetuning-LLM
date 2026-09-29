select email, age, credit_limit, lower(city) as city_lower from customers where segment = 'Consumer' and age >= 55;
