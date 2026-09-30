with base as (
    select *
    from orders
    where customer_id in (3, 29) and customer_id = 34 and customer_id in (12, 20, 25, 34)
),
order_month_summary as (
    select date_format(order_date, 'yyyy-MM') as order_month, sort_array(collect_set(status)) as status_list, max(order_date) as last_order_date
    from base
    group by date_format(order_date, 'yyyy-MM')
)
select *
from order_month_summary
order by 1
limit 1
