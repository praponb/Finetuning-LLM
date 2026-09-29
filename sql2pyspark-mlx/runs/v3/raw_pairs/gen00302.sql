select country, age + credit_limit as age_plus_credit_limit from customers where city in ('Berlin', 'Tokyo') order by country;
