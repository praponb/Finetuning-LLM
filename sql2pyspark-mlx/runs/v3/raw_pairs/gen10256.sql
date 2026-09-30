select method, success_amount
from (
    select method, sum(case when status = 'SUCCESS' then amount else 0 end) as success_amount
    from payments
    where amount is not null
    group by method
) as agg
where success_amount >= 64
order by method
