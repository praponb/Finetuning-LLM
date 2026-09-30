select id, age, credit_limit, credit_limit * age credit_limit_times_age, length(country) country_length from customers where email like '%@GMAIL.COM' order by country_length desc;
