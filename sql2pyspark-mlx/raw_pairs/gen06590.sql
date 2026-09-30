with latest as (
    select
        id,
        plan,
        end_date,
        monthly_fee,
        status
    from (
        select *, row_number() over (partition by plan order by start_date, id) as rn
        from subscriptions
    ) t
    where rn = 1
)
select
    date_trunc('month', end_date) as activity_month,
    status,
    count(case when status = 'CANCELLED' then 1 end) as cancelled_count,
    count(*) as num_rows
from latest
group by rollup (date_trunc('month', end_date), status)
order by num_rows desc
