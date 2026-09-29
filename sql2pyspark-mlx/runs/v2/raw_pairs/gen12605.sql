select avg(credit_limit) as avg_credit_limit, sum(case when city = 'Tokyo' then 1 else 0 end) as tokyo_count
from customers
where credit_limit <= 7500 and (lower(trim(name)) = 'olivia garcia' or not is_active)
order by avg_credit_limit desc, tokyo_count;
