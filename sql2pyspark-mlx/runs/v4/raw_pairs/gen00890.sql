select
    date_trunc('month', order_date) as activity_month,
    store_id,
    round(sum(discount) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as discount_per_mobile,
    sum(case when channel = 'web' then amount else 0 end) as web_amount,
    sort_array(collect_set(status)) as status_list,
    round(sum(amount) / nullif(count(case when status = 'COMPLETED' then 1 end), 0), 2) as amount_per_completed
from orders
group by grouping sets ((date_trunc('month', order_date), store_id), (date_trunc('month', order_date)))
