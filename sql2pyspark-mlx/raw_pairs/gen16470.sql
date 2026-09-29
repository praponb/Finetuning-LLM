select
    customers.id,
    customers.segment,
    customers.signup_date,
    customers.age
from customers
where customers.id in (select customer_id from sessions where customer_id in (7, 27));
