select is_active, min_credit_limit, max_age from (select is_active, min(credit_limit) as min_credit_limit, max(age) as max_age from customers group by is_active) as x where min_credit_limit >= 7000;
