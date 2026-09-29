select id, signup_date, country, name from customers where signup_date = (select max(signup_date) from customers);
