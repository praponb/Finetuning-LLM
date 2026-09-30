select * from customers qualify row_number() over (partition by country order by signup_date desc nulls first, id) = 1;
