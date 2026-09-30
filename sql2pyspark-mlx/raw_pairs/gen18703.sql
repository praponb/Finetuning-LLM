select country, sum(age) as total_age, count(*) as cnt, round(sum(age) * 100.0 / sum(sum(age)) over (), 2) as pct_of_total from customers where age between 45 and 47 group by country order by country
