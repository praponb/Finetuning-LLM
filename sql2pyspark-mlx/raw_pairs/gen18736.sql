select *, row_number() over (partition by city order by credit_limit desc, id asc) as rn from customers qualify rn = 1;
