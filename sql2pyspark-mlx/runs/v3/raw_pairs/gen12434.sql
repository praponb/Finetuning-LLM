select id, country, name, city, round(credit_limit, 2) as credit_limit_rounded from customers where signup_date between '2024-04-26' and '2025-05-01' order by id desc, 2;
