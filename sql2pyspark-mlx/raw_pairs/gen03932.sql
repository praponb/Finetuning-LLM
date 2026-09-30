select *
from (
    select *, row_number() over (partition by status order by paid_at, id) as row_num
    from payments
) as r
where row_num <= 3
order by method, amount desc;
