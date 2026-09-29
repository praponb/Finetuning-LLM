with CustomerSummary as (
    select customer_id, min(discount) as min_discount
    from orders
    group by customer_id
)
select c.id, c.signup_date, C.min_discount
from customers c
join CustomerSummary C on c.id = C.customer_id;
