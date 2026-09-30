select id, country, credit_limit, row_number() over (partition by country order by credit_limit desc, id) row_num from customers;
