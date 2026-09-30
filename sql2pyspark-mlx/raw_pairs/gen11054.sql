with numbered as (
    select *, dense_rank() over (partition by method order by paid_at desc) as rnk
    from payments
)
select id, method, paid_at
from numbered
where rnk <= 5
order by method desc;
