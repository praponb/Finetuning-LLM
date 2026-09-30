select id, segment, age from (select id, segment, age, row_number() over (partition by segment order by age, id) as rnk from customers) as t where rnk <= 5;
