select
    country,
    email,
    city,
    round(credit_limit, 1) as credit_limit_rounded
from customers
where (is_active = true and upper(trim(name)) = 'HENRY FORD') or country not in ('UK', 'US', 'DE');
