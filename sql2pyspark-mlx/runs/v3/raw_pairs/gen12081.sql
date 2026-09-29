select * from (select *, row_number() over (partition by city order by signup_date desc, id) as pos from customers where month(signup_date) in (2, 4, 5)) as ranked where pos <= 1;
