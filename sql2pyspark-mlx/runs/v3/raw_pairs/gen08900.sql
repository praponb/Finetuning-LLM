with numbered as (
    select *, rank() over (partition by status order by opened_date) as row_num
    from accounts
    where month(opened_date) in (1, 4, 6)
)
select
    id,
    status,
    opened_date,
    customer_id
from numbered
where row_num <= 3
order by customer_id desc;
