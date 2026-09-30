with Ranked as (
    select *, row_number() over (partition by customer_id order by discount, id) as rn
    from orders
)
select *
from Ranked
where rn <= 3;
