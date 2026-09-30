select year(signup_date) as signup_year, max(age) - min(age) as age_range from customers where country not in ('DE', 'TH', 'US') group by year(signup_date) having age_range < 64;
