select *
from (
    select *, row_number() over (partition by customer_id order by amount desc, id) rnk
    from orders
) ranked
where rnk <= 1
order by rnk desc;
