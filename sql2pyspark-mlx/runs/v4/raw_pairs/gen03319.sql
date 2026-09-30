with numbered as (select id, segment, age, city, row_number() over (partition by segment order by age, id) as row_num from customers) select id, segment, age, city from numbered where row_num <= 3
