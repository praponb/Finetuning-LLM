select is_active, segment, min(signup_date) as first_signup_date from customers group by is_active, segment order by segment desc, first_signup_date
