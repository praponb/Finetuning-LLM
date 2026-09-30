select * from (select *, row_number() over (partition by city order by credit_limit desc, id) as pos from customers) as t where pos = 1;
