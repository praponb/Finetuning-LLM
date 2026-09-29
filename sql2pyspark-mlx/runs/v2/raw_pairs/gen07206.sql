with numbered as (
    select *, dense_rank() over (partition by segment order by age desc) as row_num
    from customers
)
select id, segment, age
from numbered
where row_num <= 3
