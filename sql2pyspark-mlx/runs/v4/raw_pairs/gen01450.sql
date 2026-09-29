select *
from (
    select status, count(case when status = 'SHIPPED' then 1 end) as shipped_count, count(*) as cnt
    from orders
    group by status
) as x
where cnt > 6
order by status, shipped_count
