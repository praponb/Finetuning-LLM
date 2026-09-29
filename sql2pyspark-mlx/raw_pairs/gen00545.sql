select is_active, min(age) min_age, min(signup_date) first_signup_date from customers where name like 'Kan%' group by is_active having min_age < 57
