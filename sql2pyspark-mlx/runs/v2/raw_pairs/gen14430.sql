select is_active, count(age) as age_count from customers where age >= 58 and segment is not null group by is_active having age_count >= 1;
