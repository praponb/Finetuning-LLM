with filtered_payments as (
    select *
    from payments
    where paid_at < '2024-06-28'
),
paid_month_summary as (
    select date_trunc('MONTH', paid_at) as paid_month, concat_ws(', ', sort_array(collect_set(status))) as status_values
    from filtered_payments
    group by date_trunc('MONTH', paid_at)
)
select paid_month, status_values
from paid_month_summary
order by paid_month
