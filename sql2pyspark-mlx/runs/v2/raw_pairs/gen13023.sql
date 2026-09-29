select id, city, credit_limit, rank() over (partition by city order by credit_limit desc) as credit_limit_rank from customers;
