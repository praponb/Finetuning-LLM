with src as (
    select *, date_format(paid_at, 'yyyy-MM') paid_at_label
    from payments
    where method = 'cash'
),
paid_month_summary as (
    select date_format(paid_at, 'yyyy-MM') paid_month, sum(case when status = 'SUCCESS' then 1 else 0 end) success_count
    from src
    group by date_format(paid_at, 'yyyy-MM')
)
select paid_month, success_count
from paid_month_summary
where success_count < 57
order by success_count desc
