select id, name, city, is_active, credit_limit, age from customers qualify row_number() over (partition by segment order by age desc, id) = 1
