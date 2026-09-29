select id, country from customers where country != 'SG' union select id, country from customers where signup_date > '2024-04-01';
