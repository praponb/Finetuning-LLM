select country, city, name, least(credit_limit, age) as least_credit_limit_age from customers where segment = 'SMB' and lower(trim(name)) = 'emma brown' and length(email) > 6 order by name, country;
