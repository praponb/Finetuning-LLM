select id, city, credit_limit, name from (select id, city, credit_limit, name, row_number() over (partition by city order by credit_limit desc, id) as rnk from customers) as r where rnk = 1
