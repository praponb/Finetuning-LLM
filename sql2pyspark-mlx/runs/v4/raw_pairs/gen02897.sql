select status, min(amount) as min_amount
from orders
where customer_id is not null or channel <> 'web'
group by status
having sum(discount) >= 1.3
order by min_amount desc, status
limit 10;
