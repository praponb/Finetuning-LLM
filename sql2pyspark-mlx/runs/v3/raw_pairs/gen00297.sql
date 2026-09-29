select year(signup_date) as signup_year, sum(age) as total_age, round(sum(age) * 100.0 / sum(sum(age)) over (), 2) as pct_of_total from customers group by year(signup_date) order by signup_year desc
