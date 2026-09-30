select
    id,
    name,
    credit_limit,
    email,
    datediff(current_date(), signup_date) as days_since_signup_date
from customers
where year(signup_date) = 2023;
