with eligible as (
    select s.*
    from orders s
    where exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.order_id in (11, 18)
        )
)
select
    date_trunc('quarter', order_date) as quarter_start,
    sum(case when status = 'RETURNED' then amount else 0 end) as returned_amount,
    count(*) filter (where channel = 'partner') as partner_rows,
    stddev_samp(amount) as amount_stddev
from eligible
group by 1
order by returned_amount desc
