select id, segment, signup_date, name, first_value(name) over (partition by segment order by signup_date, id) as first_name from customers;
